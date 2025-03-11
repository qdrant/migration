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

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/refs"
)

const HTTPS = "https"

type MigrateFromQdrantCmd struct {
	SourceUrl              string `help:"Source GRPC URL, e.g. https://your-qdrant-hostname:6334" required:"true"`
	SourceCollection       string `help:"Source collection" required:"true"`
	SourceAPIKey           string `help:"Source API key"`
	TargetUrl              string `help:"Target GRPC URL, e.g. https://your-qdrant-hostname:6334" required:"true"`
	TargetCollection       string `help:"Target collection" required:"true"`
	TargetAPIKey           string `help:"Target API key"`
	BatchSize              uint32 `short:"b" help:"Batch size" default:"50"`
	CreateTargetCollection bool   `short:"c" help:"Create the target collection if it does not exist" default:"false"`
	MigrationMarker        string `short:"m" help:"Migration marker to resume the migration" optional:"true"`

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

func (r *MigrateFromQdrantCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := r.connect(globals, r.sourceHost, r.sourcePort, r.getSourceAPIKey(), r.sourceTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	targetClient, err := r.connect(globals, r.targetHost, r.targetPort, r.getTargetAPIKey(), r.targetTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}

	sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.SourceCollection,
		Exact:          refs.NewPointer(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err, existingIndexingThreshold := r.perpareTargetCollection(ctx, sourceClient, r.SourceCollection, targetClient, r.TargetCollection)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	sourceNonMigratedPointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.SourceCollection,
		Exact:          refs.NewPointer(true),
		Filter: &qdrant.Filter{
			MustNot: []*qdrant.Condition{
				qdrant.NewMatchKeyword("migrationMarker", r.getMigrationMarker()),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	pterm.DefaultSection.Println("Starting data migration")

	_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
		{"", "Type", "Host", "Collection", "Points"},
		{"Source", "qdrant", r.sourceHost, r.SourceCollection, strconv.FormatUint(sourcePointCount, 10)},
		{"Target", "qdrant", r.targetHost, r.TargetCollection, strconv.FormatUint(sourceNonMigratedPointCount, 10)},
	}).Render()

	err = r.migrateData(ctx, sourceClient, r.SourceCollection, targetClient, r.TargetCollection, int(sourceNonMigratedPointCount))
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.TargetCollection,
		Exact:          refs.NewPointer(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	// reset indexing threshold to enable indexing again
	err = targetClient.UpdateCollection(ctx, &qdrant.UpdateCollection{
		CollectionName: r.TargetCollection,
		OptimizersConfig: &qdrant.OptimizersConfigDiff{
			IndexingThreshold: existingIndexingThreshold,
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
		Host:        host,
		Port:        port,
		APIKey:      apiKey,
		UseTLS:      useTLS,
		TLSConfig:   &tlsConfig,
		GrpcOptions: grpcOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return client, nil
}

func (r *MigrateFromQdrantCmd) perpareTargetCollection(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string) (error, *uint64) {
	if r.CreateTargetCollection {
		sourceCollectionInfo, err := sourceClient.GetCollectionInfo(ctx, sourceCollection)
		if err != nil {
			return fmt.Errorf("failed to get source collection info: %w", err), nil
		}

		targetCollectionExists, err := targetClient.CollectionExists(ctx, targetCollection)
		if err != nil {
			return fmt.Errorf("failed to check if collection exists: %w", err), nil
		}

		if targetCollectionExists {
			fmt.Print("\n")
			pterm.Info.Printfln("Target collection already exists: %s. Skipping creation.", targetCollection)
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
				return fmt.Errorf("failed to create target collection: %w", err), nil
			}
		}
	}

	// get current indexing threshold
	targetCollectionInfo, err := targetClient.GetCollectionInfo(ctx, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get target collection information: %w", err), nil
	}

	existingIndexingThreshold := targetCollectionInfo.Config.OptimizerConfig.IndexingThreshold

	// set indexing threshold to 0 to disable indexing
	err = targetClient.UpdateCollection(ctx, &qdrant.UpdateCollection{
		CollectionName: targetCollection,
		OptimizersConfig: &qdrant.OptimizersConfigDiff{
			IndexingThreshold: refs.NewPointer(uint64(0)),
		},
	})
	if err != nil {
		return fmt.Errorf("failed disable indexing in target collection %w", err), nil
	}

	// if the threshold is 0, set it to default after wards
	if existingIndexingThreshold == nil || *existingIndexingThreshold == uint64(0) {
		existingIndexingThreshold = refs.NewPointer(uint64(20_000))
	}

	// add payload index for migration marker to source collection
	_, err = sourceClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName: sourceCollection,
		FieldName:      "migrationMarker",
		FieldType:      qdrant.FieldType_FieldTypeKeyword.Enum(),
	})
	if err != nil {
		return fmt.Errorf("failed creating index on source collection %w", err), nil
	}
	return nil, existingIndexingThreshold
}

func (r *MigrateFromQdrantCmd) migrateData(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string, sourceNonMigratedPointCount int) error {
	migrationMarker := r.getMigrationMarker()

	pterm.Info.Printfln("The migration marker value is %s. To resume the migration, add '-m %s' to the command.\n", migrationMarker, migrationMarker)

	startTime := time.Now()
	limit := r.BatchSize
	var offset *qdrant.PointId

	bar, _ := pterm.DefaultProgressbar.WithTotal(sourceNonMigratedPointCount).Start()

	for {
		resp, err := sourceClient.GetPointsClient().Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: sourceCollection,
			Offset:         offset,
			Limit:          &limit,
			WithPayload:    qdrant.NewWithPayload(true),
			WithVectors:    qdrant.NewWithVectors(true),
			Filter: &qdrant.Filter{
				MustNot: []*qdrant.Condition{
					qdrant.NewMatchKeyword("migrationMarker", migrationMarker),
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to scroll date from source: %w", err)
		}

		points := resp.GetResult()
		offset = resp.GetNextPageOffset()

		var targetPoints []*qdrant.PointStruct
		var pointIds []*qdrant.PointId

		for _, point := range points {
			targetPoints = append(targetPoints, &qdrant.PointStruct{
				Id:      point.Id,
				Payload: point.Payload,
				Vectors: point.Vectors,
			})
			pointIds = append(pointIds, point.Id)

		}

		_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: targetCollection,
			Points:         targetPoints,
		})

		if err != nil {
			return fmt.Errorf("failed to insert data into target: %w", err)
		}

		_, err = sourceClient.SetPayload(ctx, &qdrant.SetPayloadPoints{
			CollectionName: sourceCollection,
			Payload: qdrant.NewValueMap(map[string]any{
				"migrationMarker": migrationMarker,
			}),
			PointsSelector: qdrant.NewPointsSelectorIDs(pointIds),
		})

		if err != nil {
			return fmt.Errorf("failed to add migration marker: %w", err)
		}

		_ = bar.Add(int(limit))

		if offset == nil {
			break
		}

		// if one minute elapsed get updated sourcePointCount
		if time.Since(startTime) > time.Minute {
			sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
				CollectionName: sourceCollection,
				Exact:          refs.NewPointer(true),
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

func (r *MigrateFromQdrantCmd) getMigrationMarker() string {
	migrationMarker := r.MigrationMarker

	if migrationMarker == "" {
		migrationMarker = "migration-" + time.Now().Format(time.RFC3339)
	}

	return migrationMarker
}

func (r *MigrateFromQdrantCmd) getSourceAPIKey() string {
	if r.SourceAPIKey == "" {
		return os.Getenv("SOURCE_API_KEY")
	}

	return r.SourceAPIKey
}

func (r *MigrateFromQdrantCmd) getTargetAPIKey() string {
	if r.TargetAPIKey == "" {
		return os.Getenv("TARGET_API_KEY")
	}

	return r.TargetAPIKey
}
