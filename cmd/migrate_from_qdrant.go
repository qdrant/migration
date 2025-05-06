package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"

	"github.com/qdrant/go-client/qdrant"
)

const HTTPS = "https"

type MigrateFromQdrantCmd struct {
	SourceUrl                      string `help:"Source gRPC URL, e.g. https://your-qdrant-hostname:6334" required:"true"`
	SourceCollection               string `help:"Source collection" required:"true"`
	SourceAPIKey                   string `help:"Source API key" env:"SOURCE_API_KEY"`
	TargetUrl                      string `help:"Target gRPC URL, e.g. https://your-qdrant-hostname:6334" required:"true"`
	TargetCollection               string `help:"Target collection" required:"true"`
	TargetAPIKey                   string `help:"Target API key" env:"TARGET_API_KEY"`
	BatchSize                      uint32 `short:"b" help:"Batch size" default:"50"`
	CreateTargetCollection         bool   `short:"c" help:"Create the target collection if it does not exist" default:"false"`
	EnsurePayloadIndexes           bool   `help:"Ensure payload indexes are created" default:"true"`
	MigrationOffsetsCollectionName string `help:"Collection where the current migration offset should be stored" default:"_migration_offsets"`
	RestartMigration               bool   `help:"Restart the migration and do not continue from last offset" default:"false"`

	sourceHost string
	sourcePort int
	sourceTLS  bool
	targetHost string
	targetPort int
	targetTLS  bool
}

func getPort(u *url.URL) (int, error) {
	if u.Port() != "" {
		sourcePort, err := strconv.Atoi(u.Port())
		if err != nil {
			return 0, fmt.Errorf("failed to parse source port: %w", err)
		}
		return sourcePort, nil
	} else if u.Scheme == HTTPS {
		return 443, nil
	}

	return 80, nil
}

func (r *MigrateFromQdrantCmd) Parse() error {
	sourceUrl, err := url.Parse(r.SourceUrl)
	if err != nil {
		return fmt.Errorf("failed to parse source URL: %w", err)
	}

	r.sourceHost = sourceUrl.Hostname()
	r.sourceTLS = sourceUrl.Scheme == HTTPS
	r.sourcePort, err = getPort(sourceUrl)
	if err != nil {
		return fmt.Errorf("failed to parse source port: %w", err)
	}

	targetUrl, err := url.Parse(r.TargetUrl)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	r.targetHost = targetUrl.Hostname()
	r.targetTLS = targetUrl.Scheme == HTTPS
	r.targetPort, err = getPort(targetUrl)
	if err != nil {
		return fmt.Errorf("failed to parse source port: %w", err)
	}

	return nil
}

func (r *MigrateFromQdrantCmd) Validate() error {
	if r.BatchSize < 1 {
		return fmt.Errorf("batch size must be greater than 0")
	}

	return nil
}

func (r *MigrateFromQdrantCmd) ValidateParsedValues() error {
	if r.sourceHost == r.targetHost && r.sourcePort == r.targetPort && r.SourceCollection == r.TargetCollection {
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

	sourceClient, err := r.connect(globals, r.sourceHost, r.sourcePort, r.SourceAPIKey, r.sourceTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	targetClient, err := r.connect(globals, r.targetHost, r.targetPort, r.TargetAPIKey, r.targetTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}

	err = r.prepareMigrationOffsetsCollection(ctx, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.SourceCollection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	existingM, err := r.perpareTargetCollection(ctx, sourceClient, r.SourceCollection, targetClient, r.TargetCollection)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.TargetCollection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	pterm.DefaultSection.Println("Starting data migration")

	_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
		{"Type", "Provider", "Collection", "Points"},
		{"Source", "qdrant", r.SourceCollection, strconv.FormatUint(sourcePointCount, 10)},
		{"Target", "qdrant", r.TargetCollection, strconv.FormatUint(targetPointCount, 10)},
	}).Render()

	err = r.migrateData(ctx, sourceClient, r.SourceCollection, targetClient, r.TargetCollection, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err = targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.TargetCollection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	// reset m to enable indexing again
	err = targetClient.UpdateCollection(ctx, &qdrant.UpdateCollection{
		CollectionName: r.TargetCollection,
		HnswConfig: &qdrant.HnswConfigDiff{
			M: existingM,
		},
	})
	if err != nil {
		return fmt.Errorf("failed disable indexing in target collection %w", err)
	}

	pterm.Info.Printfln("Target collection has %d points\n", targetPointCount)

	return nil
}

func (r *MigrateFromQdrantCmd) connect(globals *Globals, host string, port int, apiKey string, useTLS bool) (*qdrant.Client, error) {
	debugLogger := logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		pterm.Debug.Printf(msg, fields...)
	})

	var grpcOptions []grpc.DialOption

	if globals.Trace {
		pterm.EnableDebugMessages()
		loggingOptions := logging.WithLogOnEvents(logging.StartCall, logging.FinishCall, logging.PayloadSent, logging.PayloadReceived)
		grpcOptions = append(grpcOptions, grpc.WithChainUnaryInterceptor(logging.UnaryClientInterceptor(debugLogger, loggingOptions)))
		grpcOptions = append(grpcOptions, grpc.WithChainStreamInterceptor(logging.StreamClientInterceptor(debugLogger, loggingOptions)))
	}
	if globals.Debug {
		pterm.EnableDebugMessages()
		loggingOptions := logging.WithLogOnEvents(logging.StartCall, logging.FinishCall)
		grpcOptions = append(grpcOptions, grpc.WithChainUnaryInterceptor(logging.UnaryClientInterceptor(debugLogger, loggingOptions)))
		grpcOptions = append(grpcOptions, grpc.WithChainStreamInterceptor(logging.StreamClientInterceptor(debugLogger, loggingOptions)))
	}

	tlsConfig := tls.Config{
		InsecureSkipVerify: globals.SkipTlsVerification,
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   host,
		Port:                   port,
		APIKey:                 apiKey,
		UseTLS:                 useTLS,
		TLSConfig:              &tlsConfig,
		GrpcOptions:            grpcOptions,
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return client, nil
}

func (r *MigrateFromQdrantCmd) prepareMigrationOffsetsCollection(ctx context.Context, targetClient *qdrant.Client) error {
	migrationOffsetCollectionExists, err := targetClient.CollectionExists(ctx, r.MigrationOffsetsCollectionName)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if migrationOffsetCollectionExists {
		return nil
	}
	return targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName:    r.MigrationOffsetsCollectionName,
		ReplicationFactor: qdrant.PtrOf(uint32(1)),
		ShardNumber:       qdrant.PtrOf(uint32(1)),
		VectorsConfig:     qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{}),
		StrictModeConfig: &qdrant.StrictModeConfig{
			Enabled: qdrant.PtrOf(false),
		},
	})
}

func (r *MigrateFromQdrantCmd) perpareTargetCollection(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string) (*uint64, error) {
	sourceCollectionInfo, err := sourceClient.GetCollectionInfo(ctx, sourceCollection)
	if err != nil {
		return nil, fmt.Errorf("failed to get source collection info: %w", err)
	}

	if r.CreateTargetCollection {
		targetCollectionExists, err := targetClient.CollectionExists(ctx, targetCollection)
		if err != nil {
			return nil, fmt.Errorf("failed to check if collection exists: %w", err)
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
				return nil, fmt.Errorf("failed to create target collection: %w", err)
			}
		}
	}

	// get current m
	targetCollectionInfo, err := targetClient.GetCollectionInfo(ctx, targetCollection)
	if err != nil {
		return nil, fmt.Errorf("failed to get target collection information: %w", err)
	}
	existingM := targetCollectionInfo.Config.HnswConfig.M

	// set m to 0 to disable indexing
	err = targetClient.UpdateCollection(ctx, &qdrant.UpdateCollection{
		CollectionName: targetCollection,
		HnswConfig: &qdrant.HnswConfigDiff{
			M: qdrant.PtrOf(uint64(0)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed disable indexing in target collection %w", err)
	}

	// if m is 0, set it to default after wards
	if existingM == nil || *existingM == uint64(0) {
		existingM = qdrant.PtrOf(uint64(16))
	}

	// add payload index for migration marker to source collection
	_, err = sourceClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName: sourceCollection,
		FieldName:      "migrationMarker",
		FieldType:      qdrant.FieldType_FieldTypeKeyword.Enum(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed creating index on source collection %w", err)
	}

	if r.EnsurePayloadIndexes {
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
					CollectionName:   r.TargetCollection,
					FieldName:        name,
					FieldType:        fieldType,
					FieldIndexParams: schemaInfo.GetParams(),
					Wait:             qdrant.PtrOf(true),
				},
			)
			if err != nil {
				return nil, fmt.Errorf("failed creating index on tagrget collection %w", err)
			}
		}
	}

	return existingM, nil
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
	startTime := time.Now()
	limit := r.BatchSize
	offset, offsetCount, err := r.getStartOffset(ctx, targetClient, sourceCollection)
	if err != nil {
		return fmt.Errorf("failed to get start offset: %w", err)
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	if offset != nil {
		pterm.Info.Printfln("Starting from offset %s (%d)", offset, offsetCount)
		bar.Add(int(offsetCount))
	} else {
		pterm.Info.Printfln("Starting from beginning")
	}

	fmt.Print("\n")

	for {
		resp, err := sourceClient.GetPointsClient().Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: sourceCollection,
			Offset:         offset,
			Limit:          &limit,
			WithPayload:    qdrant.NewWithPayload(true),
			WithVectors:    qdrant.NewWithVectors(true),
		})
		if err != nil {
			return fmt.Errorf("failed to scroll date from source: %w", err)
		}

		points := resp.GetResult()
		offset = resp.GetNextPageOffset()

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

		err = r.storeStartOffset(ctx, targetClient, sourceCollection, offset, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(points))

		if offset == nil {
			break
		}

		// if one minute elapsed get updated sourcePointCount
		if time.Since(startTime) > time.Minute {
			sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
				CollectionName: sourceCollection,
				Exact:          qdrant.PtrOf(true),
			})
			if err != nil {
				return fmt.Errorf("failed to count points in source: %w", err)
			}
			bar.Total = int(sourcePointCount)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")

	return nil
}

func (r *MigrateFromQdrantCmd) getStartOffset(ctx context.Context, targetClient *qdrant.Client, sourceCollection string) (*qdrant.PointId, uint64, error) {
	if r.RestartMigration {
		return nil, 0, nil
	}
	point, err := r.getOffsetPoint(ctx, targetClient, sourceCollection)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get start offset point: %w", err)
	}
	if point == nil {
		return nil, 0, nil
	}
	offset, ok := point.Payload[sourceCollection+"_offset"]
	if !ok {
		return nil, 0, nil
	}
	offsetCount, ok := point.Payload[sourceCollection+"_offsetCount"]
	if !ok {
		return nil, 0, nil
	}

	offsetCountValue, ok := offsetCount.GetKind().(*qdrant.Value_IntegerValue)
	if !ok {
		return nil, 0, fmt.Errorf("failed to get offset count: %w", err)
	}

	offsetIntegerValue, ok := offset.GetKind().(*qdrant.Value_IntegerValue)
	if ok {
		return qdrant.NewIDNum(uint64(offsetIntegerValue.IntegerValue)), uint64(offsetCountValue.IntegerValue), nil
	}

	offsetStringValue, ok := offset.GetKind().(*qdrant.Value_StringValue)
	if ok {
		return qdrant.NewIDUUID(offsetStringValue.StringValue), uint64(offsetCountValue.IntegerValue), nil
	}

	return nil, 0, nil
}

func getPointID(offset *qdrant.PointId) (interface{}, error) {
	switch pointID := offset.GetPointIdOptions().(type) {
	case *qdrant.PointId_Num:
		return pointID.Num, nil
	case *qdrant.PointId_Uuid:
		return pointID.Uuid, nil
	default:
		return nil, fmt.Errorf("unsupported offset type: %T", pointID)
	}
}

func (r *MigrateFromQdrantCmd) storeStartOffset(ctx context.Context, targetClient *qdrant.Client, sourceCollection string, offset *qdrant.PointId, offsetCount uint64) error {
	if offset == nil {
		return nil
	}
	offsetId, err := getPointID(offset)
	if err != nil {
		return err
	}

	payload := qdrant.NewValueMap(map[string]any{
		sourceCollection + "_offset":       offsetId,
		sourceCollection + "_offsetCount":  offsetCount,
		sourceCollection + "_lastUpsertAt": time.Now().Format("2006-01-02 15:04:05"),
	})

	_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: r.MigrationOffsetsCollectionName,
		Points: []*qdrant.PointStruct{
			{
				Id:      r.getOffsetPointId(sourceCollection),
				Payload: payload,
				Vectors: qdrant.NewVectorsMap(map[string]*qdrant.Vector{}),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to store offset: %w", err)
	}
	return nil
}

func (r *MigrateFromQdrantCmd) getOffsetPoint(ctx context.Context, targetClient *qdrant.Client, sourceCollection string) (*qdrant.RetrievedPoint, error) {
	points, err := targetClient.Get(ctx, &qdrant.GetPoints{
		CollectionName: r.MigrationOffsetsCollectionName,
		Ids:            []*qdrant.PointId{r.getOffsetPointId(sourceCollection)},
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get start offset: %w", err)
	}
	if len(points) == 0 {
		return nil, nil
	}

	return points[0], nil
}

func (r *MigrateFromQdrantCmd) getOffsetPointId(sourceCollection string) *qdrant.PointId {
	deterministicUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(sourceCollection))

	return qdrant.NewIDUUID(deterministicUUID.String())
}
