package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/aws/smithy-go/document"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromS3VectorsCmd struct {
	S3        commons.S3VectorsConfig `embed:"" prefix:"s3."`
	Qdrant    commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField   string                  `prefix:"qdrant." help:"Field storing S3 IDs in Qdrant." default:"__id__"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromS3VectorsCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromS3VectorsCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromS3VectorsCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("S3 Vectors to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := r.connectToS3Vectors()
	if err != nil {
		return fmt.Errorf("failed to connect to S3 Vectors source: %w", err)
	}

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceClient, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("s3-vectors", fmt.Sprintf("%s/%s", r.S3.Bucket, r.S3.Index), r.Qdrant.Collection)

	err = r.migrateData(ctx, sourceClient, targetClient)
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

	return nil
}

func (r *MigrateFromS3VectorsCmd) connectToS3Vectors() (*s3vectors.Client, error) {
	ctx := context.Background()
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return s3vectors.NewFromConfig(sdkConfig), nil
}

func (r *MigrateFromS3VectorsCmd) prepareTargetCollection(ctx context.Context, sourceClient *s3vectors.Client, targetClient *qdrant.Client) error {
	if !r.Migration.CreateCollection {
		return nil
	}

	targetCollectionExists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}

	if targetCollectionExists {
		pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}

	indexInfo, err := sourceClient.GetIndex(ctx, &s3vectors.GetIndexInput{
		IndexName:        &r.S3.Index,
		VectorBucketName: &r.S3.Bucket,
	})
	if err != nil {
		return fmt.Errorf("failed to get S3 Vectors index: %w", err)
	}

	distanceMapping := map[types.DistanceMetric]qdrant.Distance{
		types.DistanceMetricCosine:    qdrant.Distance_Cosine,
		types.DistanceMetricEuclidean: qdrant.Distance_Euclid,
	}

	dist, ok := distanceMapping[indexInfo.Index.DistanceMetric]
	if !ok {
		return fmt.Errorf("unsupported distance metric: %v", indexInfo.Index.DistanceMetric)
	}

	createReq := &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(*indexInfo.Index.Dimension),
			Distance: dist,
		}),
	}

	if err := targetClient.CreateCollection(ctx, createReq); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection '%s'", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromS3VectorsCmd) migrateData(ctx context.Context, sourceClient *s3vectors.Client, targetClient *qdrant.Client) error {
	batchSize := r.Migration.BatchSize

	var offsetId *qdrant.PointId
	offsetCount := uint64(0)

	sourceIdentifier := fmt.Sprintf("%s/%s", r.S3.Bucket, r.S3.Index)

	if !r.Migration.Restart {
		id, offsetStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceIdentifier)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = offsetStored
		offsetId = id
	}

	spinner, _ := pterm.DefaultSpinner.Start("Migrating data")
	if offsetCount > 0 {
		spinner.UpdateText(fmt.Sprintf("Resuming migration from %d points", offsetCount))
	}

	for {
		limit := int32(batchSize)
		var nextToken *string
		if offsetId != nil {
			token := offsetId.GetUuid()
			nextToken = &token
		}
		listRes, err := sourceClient.ListVectors(ctx, &s3vectors.ListVectorsInput{
			IndexName:        &r.S3.Index,
			VectorBucketName: &r.S3.Bucket,
			MaxResults:       &limit,
			NextToken:        nextToken,
			ReturnData:       true,
			ReturnMetadata:   true,
		})
		if err != nil {
			return fmt.Errorf("failed to list vectors from S3 Vectors: %w", err)
		}

		if len(listRes.Vectors) == 0 {
			break
		}

		var targetPoints []*qdrant.PointStruct
		for _, vec := range listRes.Vectors {
			point := &qdrant.PointStruct{
				Id: arbitraryIDToUUID(*vec.Key),
			}

			vData, ok := vec.Data.(*types.VectorDataMemberFloat32)
			if !ok {
				return fmt.Errorf("unexpected vector data type")
			}

			point.Vectors = qdrant.NewVectorsDense(vData.Value)

			payload := make(map[string]*qdrant.Value)
			if vec.Metadata != nil {
				var metaMap map[string]any
				err = vec.Metadata.UnmarshalSmithyDocument(&metaMap)
				if err != nil {
					return fmt.Errorf("failed to unmarshal metadata: %w", err)
				}
				metaMap = convertSmithyDocumentNumbers(metaMap).(map[string]any)
				payload = qdrant.NewValueMap(metaMap)
			}
			payload[r.IdField] = qdrant.NewValueString(*vec.Key)
			point.Payload = payload

			targetPoints = append(targetPoints, point)
		}

		_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return fmt.Errorf("failed to insert data into target: %w", err)
		}

		offsetCount += uint64(len(targetPoints))
		if listRes.NextToken != nil {
			offsetId = qdrant.NewID(*listRes.NextToken)
			err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceIdentifier, offsetId, offsetCount)
			if err != nil {
				return fmt.Errorf("failed to store offset: %w", err)
			}
		}

		spinner.UpdateText(fmt.Sprintf("Migrated %d points", offsetCount))

		if listRes.NextToken == nil {
			break
		}
	}

	spinner.Success("Migration finished successfully")
	return nil
}

// Recursively converts document.Number values to float64 or int64 for Qdrant compatibility.
func convertSmithyDocumentNumbers(v any) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case map[string]any:
		res := make(map[string]any, len(val))
		for k, v2 := range val {
			res[k] = convertSmithyDocumentNumbers(v2)
		}
		return res
	case []any:
		res := make([]any, len(val))
		for i, v2 := range val {
			res[i] = convertSmithyDocumentNumbers(v2)
		}
		return res
	case document.Number:
		// Try int64 first, fallback to float64
		if i, err := val.Int64(); err == nil {
			return i
		}
		if f, err := val.Float64(); err == nil {
			return f
		}
		return val.String() // fallback: string
	default:
		return v
	}
}
