package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromQdrantCmd struct {
	Source    commons.QdrantConfig    `embed:"" prefix:"source."`
	Target    commons.QdrantConfig    `embed:"" prefix:"target."`
	Migration commons.MigrationConfig `embed:"" prefix:"migration."`

	sourceHost string
	sourcePort int
	sourceTLS  bool
	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromQdrantCmd) Parse() error {
	var err error
	r.sourceHost, r.sourcePort, r.sourceTLS, err = parseQdrantUrl(r.Source.Url)
	if err != nil {
		return fmt.Errorf("failed to parse source URL: %w", err)
	}

	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Target.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromQdrantCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromQdrantCmd) ValidateParsedValues() error {
	if r.sourceHost == r.targetHost && r.sourcePort == r.targetPort && r.Source.Collection == r.Target.Collection {
		return fmt.Errorf("source and target collections must be different")
	}

	return nil
}

func (r *MigrateFromQdrantCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}
	err = r.ValidateParsedValues()
	if err != nil {
		return fmt.Errorf("failed to validate input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := connectToQdrant(globals, r.sourceHost, r.sourcePort, r.Source.APIKey, r.sourceTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Target.APIKey, r.targetTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Source.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.perpareTargetCollection(ctx, sourceClient, r.Source.Collection, targetClient, r.Target.Collection)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("qdrant", r.Source.Collection, r.Target.Collection)

	err = r.migrateData(ctx, sourceClient, r.Source.Collection, targetClient, r.Target.Collection, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Target.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	pterm.Info.Printfln("Target collection has %d points\n", targetPointCount)

	return nil
}

func (r *MigrateFromQdrantCmd) perpareTargetCollection(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string) error {
	sourceCollectionInfo, err := sourceClient.GetCollectionInfo(ctx, sourceCollection)
	if err != nil {
		return fmt.Errorf("failed to get source collection info: %w", err)
	}

	if r.Migration.CreateCollection {
		targetCollectionExists, err := targetClient.CollectionExists(ctx, targetCollection)
		if err != nil {
			return fmt.Errorf("failed to check if collection exists: %w", err)
		}

		if targetCollectionExists {
			fmt.Print("\n")
			pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", targetCollection)
		} else {
			err = targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
				CollectionName:         targetCollection,
				HnswConfig:             sourceCollectionInfo.Config.GetHnswConfig(),
				WalConfig:              sourceCollectionInfo.Config.GetWalConfig(),
				OptimizersConfig:       sourceCollectionInfo.Config.GetOptimizerConfig(),
				ShardNumber:            &sourceCollectionInfo.Config.GetParams().ShardNumber,
				OnDiskPayload:          &sourceCollectionInfo.Config.GetParams().OnDiskPayload,
				VectorsConfig:          sourceCollectionInfo.Config.GetParams().VectorsConfig,
				ReplicationFactor:      sourceCollectionInfo.Config.GetParams().ReplicationFactor,
				WriteConsistencyFactor: sourceCollectionInfo.Config.GetParams().WriteConsistencyFactor,
				QuantizationConfig:     sourceCollectionInfo.Config.GetQuantizationConfig(),
				ShardingMethod:         sourceCollectionInfo.Config.GetParams().ShardingMethod,
				SparseVectorsConfig:    sourceCollectionInfo.Config.GetParams().SparseVectorsConfig,
				StrictModeConfig:       sourceCollectionInfo.Config.GetStrictModeConfig(),
			})
			if err != nil {
				return fmt.Errorf("failed to create target collection: %w", err)
			}
		}
	}

	targetCollectionInfo, err := targetClient.GetCollectionInfo(ctx, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get target collection information: %w", err)
	}

	if r.Migration.EnsurePayloadIndexes {
		for name, schemaInfo := range sourceCollectionInfo.GetPayloadSchema() {
			fieldType := getFieldType(schemaInfo.GetDataType())
			if fieldType == nil {
				continue
			}

			// if there is already an index in the target collection, skip
			if _, ok := targetCollectionInfo.GetPayloadSchema()[name]; ok {
				continue
			}

			_, err = targetClient.CreateFieldIndex(
				ctx,
				&qdrant.CreateFieldIndexCollection{
					CollectionName:   r.Target.Collection,
					FieldName:        name,
					FieldType:        fieldType,
					FieldIndexParams: schemaInfo.GetParams(),
					Wait:             qdrant.PtrOf(true),
				},
			)
			if err != nil {
				return fmt.Errorf("failed creating index on tagrget collection %w", err)
			}
		}
	}

	return nil
}

func getFieldType(dataType qdrant.PayloadSchemaType) *qdrant.FieldType {
	switch dataType {
	case qdrant.PayloadSchemaType_Keyword:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeKeyword)
	case qdrant.PayloadSchemaType_Integer:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeInteger)
	case qdrant.PayloadSchemaType_Float:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeFloat)
	case qdrant.PayloadSchemaType_Geo:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeGeo)
	case qdrant.PayloadSchemaType_Text:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeText)
	case qdrant.PayloadSchemaType_Bool:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeBool)
	case qdrant.PayloadSchemaType_Datetime:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeDatetime)
	case qdrant.PayloadSchemaType_Uuid:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeUuid)
	}
	return nil
}

func (r *MigrateFromQdrantCmd) migrateData(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string, sourcePointCount uint64) error {
	limit := uint32(r.Migration.BatchSize)

	var offsetId *qdrant.PointId
	offsetCount := uint64(0)

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceCollection)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetId = id
		offsetCount = count
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		resp, err := sourceClient.GetPointsClient().Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: sourceCollection,
			Offset:         offsetId,
			Limit:          &limit,
			WithPayload:    qdrant.NewWithPayload(true),
			WithVectors:    qdrant.NewWithVectors(true),
		})
		if err != nil {
			return fmt.Errorf("failed to scroll date from source: %w", err)
		}

		points := resp.GetResult()
		offsetId = resp.GetNextPageOffset()

		var targetPoints []*qdrant.PointStruct
		getVector := func(vector *qdrant.VectorOutput) *qdrant.Vector {
			if vector == nil {
				return nil
			}
			return &qdrant.Vector{
				Data:         vector.GetData(),
				Indices:      vector.GetIndices(),
				VectorsCount: vector.VectorsCount,
			}
		}
		getNamedVectors := func(vectors map[string]*qdrant.VectorOutput) map[string]*qdrant.Vector {
			result := make(map[string]*qdrant.Vector, len(vectors))
			for k, v := range vectors {
				result[k] = getVector(v)
			}
			return result
		}
		getVectors := func(vectors *qdrant.NamedVectorsOutput) *qdrant.NamedVectors {
			if vectors == nil {
				return nil
			}
			return &qdrant.NamedVectors{
				Vectors: getNamedVectors(vectors.GetVectors()),
			}
		}
		getVectorsFromPoint := func(point *qdrant.RetrievedPoint) *qdrant.Vectors {
			if point.Vectors == nil {
				return nil
			}
			if vector := point.Vectors.GetVector(); vector != nil {
				return &qdrant.Vectors{
					VectorsOptions: &qdrant.Vectors_Vector{
						Vector: getVector(vector),
					},
				}
			}
			if vectors := point.Vectors.GetVectors(); vectors != nil {
				return &qdrant.Vectors{
					VectorsOptions: &qdrant.Vectors_Vectors{
						Vectors: getVectors(vectors),
					},
				}
			}
			return nil
		}
		for _, point := range points {
			targetPoints = append(targetPoints, &qdrant.PointStruct{
				Id:      point.Id,
				Payload: point.Payload,
				Vectors: getVectorsFromPoint(point),
			})
		}

		_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: targetCollection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})

		if err != nil {
			return fmt.Errorf("failed to insert data into target: %w", err)
		}

		offsetCount += uint64(len(points))

		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceCollection, offsetId, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(points))

		if offsetId == nil {
			break
		}

	}

	pterm.Success.Printfln("Data migration finished successfully")

	return nil
}
