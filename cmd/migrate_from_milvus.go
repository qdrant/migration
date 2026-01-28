package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromMilvusCmd struct {
	Milvus         commons.MilvusConfig    `embed:"" prefix:"milvus."`
	Qdrant         commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration      commons.MigrationConfig `embed:"" prefix:"migration."`
	DistanceMetric map[string]string       `prefix:"qdrant." help:"Map of vector field names to distance metrics (cosine,dot,euclid,manhattan). Default is cosine if not specified."`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromMilvusCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromMilvusCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromMilvusCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Milvus to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := r.connectToMilvus(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Milvus source: %w", err)
	}

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countMilvusVectors(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceClient, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("milvus", r.Milvus.Collection, r.Qdrant.Collection)

	err = r.migrateData(ctx, sourceClient, targetClient, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Qdrant.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	pterm.Info.Printfln("Target collection has %d points\n", targetPointCount)

	err = commons.DeleteOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}

	return nil
}

func (r *MigrateFromMilvusCmd) connectToMilvus(ctx context.Context) (*milvusclient.Client, error) {
	client, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address:       r.Milvus.Url,
		APIKey:        r.Milvus.APIKey,
		EnableTLSAuth: r.Milvus.EnableTLSAuth,
		Username:      r.Milvus.Username,
		Password:      r.Milvus.Password,
		DBName:        r.Milvus.DBName,
		ServerVersion: r.Milvus.ServerVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Milvus client: %w", err)
	}

	return client, nil
}

func (r *MigrateFromMilvusCmd) countMilvusVectors(ctx context.Context, client *milvusclient.Client) (uint64, error) {
	stats, err := client.GetCollectionStats(ctx, milvusclient.NewGetCollectionStatsOption(r.Milvus.Collection))
	if err != nil {
		return 0, fmt.Errorf("failed to get collection statistics: %w", err)
	}

	count, err := strconv.ParseUint(stats["row_count"], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse row count: %w", err)
	}

	return count, nil
}

func (r *MigrateFromMilvusCmd) prepareTargetCollection(ctx context.Context, sourceClient *milvusclient.Client, targetClient *qdrant.Client) error {
	if !r.Migration.CreateCollection {
		return nil
	}

	targetCollectionExists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}

	if targetCollectionExists {
		pterm.Info.Printfln("Target collection %q already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}

	schema, err := sourceClient.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(r.Milvus.Collection))
	if err != nil {
		return fmt.Errorf("failed to describe Milvus collection: %w", err)
	}

	distanceMapping := map[string]qdrant.Distance{
		"euclid":    qdrant.Distance_Euclid,
		"cosine":    qdrant.Distance_Cosine,
		"dot":       qdrant.Distance_Dot,
		"manhattan": qdrant.Distance_Manhattan,
	}

	vectorParamsMap := make(map[string]*qdrant.VectorParams)
	for _, field := range schema.Schema.Fields {
		if field.DataType == entity.FieldTypeFloatVector {
			dim := field.TypeParams["dim"]
			dimension, err := strconv.ParseUint(dim, 10, 32)
			if err != nil {
				return fmt.Errorf("failed to parse vector dimension: %w", err)
			}

			distanceMetric := "cosine"
			if specifiedDistance, ok := r.DistanceMetric[field.Name]; ok {
				if _, valid := distanceMapping[specifiedDistance]; !valid {
					return fmt.Errorf("invalid distance metric '%s' for vector '%s'", specifiedDistance, field.Name)
				}
				distanceMetric = specifiedDistance
			}

			vectorParamsMap[field.Name] = &qdrant.VectorParams{
				Size:     uint64(dimension),
				Distance: distanceMapping[distanceMetric],
			}
		}
	}

	err = targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig:  qdrant.NewVectorsConfigMap(vectorParamsMap),
	})
	if err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection %q", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromMilvusCmd) migrateData(ctx context.Context, sourceClient *milvusclient.Client, targetClient *qdrant.Client, sourcePointCount uint64) error {
	batchSize := r.Migration.BatchSize

	var offsetID *qdrant.PointId
	offsetCount := uint64(0)
	var err error

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Milvus.Collection)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
		offsetID = id
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	schema, err := sourceClient.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(r.Milvus.Collection))
	if err != nil {
		return fmt.Errorf("describe collection failed: %w", err)
	}

	pkField := schema.Schema.PKField()
	pkName := pkField.Name
	pkType := pkField.DataType

	for {
		filter := ""
		if offsetID != nil {
			switch pkType {
			case entity.FieldTypeInt64:
				filter = fmt.Sprintf("%s > %d", pkName, offsetID.GetNum())
			case entity.FieldTypeVarChar:
				filter = fmt.Sprintf("%s > '%s'", pkName, offsetID.GetUuid())
			}
		}

		result, err := sourceClient.Query(ctx, milvusclient.NewQueryOption(r.Milvus.Collection).
			WithPartitions(r.Milvus.Partitions...).
			WithFilter(filter).
			WithOutputFields("*").
			WithLimit(batchSize))

		if err != nil {
			return fmt.Errorf("failed to query Milvus: %w", err)
		}

		if result.ResultCount == 0 {
			break
		}

		var targetPoints []*qdrant.PointStruct
		for i := 0; i < result.ResultCount; i++ {
			point := &qdrant.PointStruct{}
			vectors := make(map[string]*qdrant.Vector)
			payload := make(map[string]interface{})

			for _, col := range result.Fields {
				fieldName := col.Name()
				value, err := extractValue(col, i)
				if err != nil {
					return fmt.Errorf("failed to extract value: %w", err)
				}

				if fieldName == pkName {
					switch col.Type() {
					case entity.FieldTypeVarChar:
						uuid := value.(string)
						offsetID = qdrant.NewID(uuid)
						point.Id = offsetID
					case entity.FieldTypeInt64:
						num := value.(int64)
						offsetID = qdrant.NewIDNum(uint64(num))
						point.Id = offsetID
					}
					continue
				}

				switch col.Type() {
				case entity.FieldTypeFloatVector:
					if value, ok := value.([]float32); ok {
						vectors[fieldName] = qdrant.NewVector(value...)
					}
				default:
					payload[fieldName] = value
				}
			}

			if len(vectors) > 0 {
				point.Vectors = qdrant.NewVectorsMap(vectors)
			}
			point.Payload = qdrant.NewValueMap(payload)
			targetPoints = append(targetPoints, point)
		}

		err = upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return err
		}

		offsetCount += uint64(len(targetPoints))
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Milvus.Collection, offsetID, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))

		// Apply batch delay if configured (helps with rate limiting)
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")

	return nil
}

func extractValue(col column.Column, index int) (interface{}, error) {
	data := col.FieldData()

	switch col.Type() {
	case entity.FieldTypeBool:
		return data.GetScalars().GetBoolData().GetData()[index], nil

	case entity.FieldTypeInt64:
		return data.GetScalars().GetLongData().GetData()[index], nil

	case entity.FieldTypeInt8, entity.FieldTypeInt16, entity.FieldTypeInt32:
		return int64(data.GetScalars().GetIntData().GetData()[index]), nil

	case entity.FieldTypeFloat:
		return data.GetScalars().GetFloatData().GetData()[index], nil

	case entity.FieldTypeDouble:
		return data.GetScalars().GetDoubleData().GetData()[index], nil

	case entity.FieldTypeVarChar, entity.FieldTypeString:
		return data.GetScalars().GetStringData().GetData()[index], nil

	case entity.FieldTypeJSON:
		jsonData := data.GetScalars().GetJsonData().GetData()[index]
		var result map[string]interface{}
		if err := json.Unmarshal(jsonData, &result); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		return result, nil

	case entity.FieldTypeFloatVector:
		vec := data.GetVectors().GetFloatVector().GetData()
		dim := int(data.GetVectors().Dim)
		start := index * dim
		end := start + dim
		return vec[start:end], nil

	// TODO(Anush008): Extract sparse vectors when it's out of beta in Milvus.
	// https://github.com/milvus-io/milvus-proto/blob/02ce2e62a9fd3053b5f2dc632aea32d289a562da/proto/schema.proto#L165
	default:
		return nil, fmt.Errorf("unsupported field type: %v", col.Type())
	}
}
