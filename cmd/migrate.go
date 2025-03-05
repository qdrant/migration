package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/refs"
)

func parseConnectionString(connStr string) (connectionType string, host string, port int, collection string, tls bool, apiKey string, err error) {
	r, err := regexp.Compile(`^(?P<connectionType>\w+):///(?P<protocol>(http|https))://(?P<host>[\w-.]+):(?P<port>\d+)/(?P<collection>[\w.-]+)(\?apiKey=(?P<apiKey>.+))?$`)
	if err != nil {
		return "", "", 0, "", false, "", fmt.Errorf("failed to compile regexp: %w", err)
	}

	m := r.FindStringSubmatch(connStr)
	if m == nil {
		return "", "", 0, "", false, "", fmt.Errorf("failed to parse connection string: %s", connStr)
	}
	var protocol, foundPort string

	for i, name := range r.SubexpNames() {
		switch name {
		case "connectionType":
			connectionType = m[i]
		case "protocol":
			protocol = m[i]
		case "host":
			host = m[i]
		case "port":
			foundPort = m[i]
		case "collection":
			collection = m[i]
		case "apiKey":
			apiKey = m[i]
		}
	}

	port, err = strconv.Atoi(foundPort)

	if err != nil {
		return "", "", 0, "", false, "", fmt.Errorf("failed to parse port: %s: %w", foundPort, err)
	}

	return connectionType, host, port, collection, protocol == "https", apiKey, nil
}

type MigrateCmd struct {
	Source                 string `short:"s" help:"Source collection" required:"true"`
	Target                 string `short:"t" help:"Target collection" required:"true"`
	BatchSize              uint32 `short:"b" help:"Batch size" default:"50"`
	CreateTargetCollection bool   `short:"c" help:"Create the target collection if it does not exist" default:"false"`
	MigrationMarker        string `short:"m" help:"Migration marker to resume the migration" optional:"true"`
}

func (r *MigrateCmd) Validate() error {
	if r.BatchSize < 1 {
		return fmt.Errorf("batch size must be greater than 0")
	}

	return nil
}

func (r *MigrateCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Qdrant Data Migration")

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

	pterm.Debug.Printf("test")

	ctx := context.Background()

	sourceType, sourceHost, sourcePort, sourceCollection, sourceTLS, sourceAPIKey, err := parseConnectionString(r.Source)
	if err != nil {
		return fmt.Errorf("failed to parse source connection string: %w", err)
	}
	targetType, targetHost, targetPort, targetCollection, targetTLS, targetAPIKey, err := parseConnectionString(r.Target)
	if err != nil {
		return fmt.Errorf("failed to parse target connection string: %w", err)
	}

	if sourceType != "qdrant" {
		return fmt.Errorf("unsupported source type: %s", sourceType)
	}
	if targetType != "qdrant" {
		return fmt.Errorf("unsupported target type: %s", targetType)
	}

	tlsConfig := tls.Config{
		InsecureSkipVerify: true,
	}

	sourceClient, err := qdrant.NewClient(&qdrant.Config{
		Host:        sourceHost,
		Port:        sourcePort,
		APIKey:      sourceAPIKey,
		UseTLS:      sourceTLS,
		TLSConfig:   &tlsConfig,
		GrpcOptions: grpcOptions,
	})
	if err != nil {
		return fmt.Errorf("failed to create source client: %w", err)
	}

	targetClient, err := qdrant.NewClient(&qdrant.Config{
		Host:        targetHost,
		Port:        targetPort,
		APIKey:      targetAPIKey,
		UseTLS:      targetTLS,
		TLSConfig:   &tlsConfig,
		GrpcOptions: grpcOptions,
	})
	if err != nil {
		return fmt.Errorf("failed to create target client: %w", err)
	}

	migrationMarker := r.MigrationMarker

	if migrationMarker == "" {
		migrationMarker = "migration-" + time.Now().Format(time.RFC3339)
	}

	startTime := time.Now()

	exactPointCount := true

	sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: sourceCollection,
		Exact:          &exactPointCount,
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	if r.CreateTargetCollection {
		sourceCollectionInfo, err := sourceClient.GetCollectionInfo(ctx, sourceCollection)
		if err != nil {
			return fmt.Errorf("failed to get source collection info: %w", err)
		}

		targetCollectionExists, err := targetClient.CollectionExists(ctx, targetCollection)

		if err != nil {
			return fmt.Errorf("failed to check if collection exists: %w", err)
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
				return fmt.Errorf("failed to create target collection: %w", err)
			}
		}
	}

	// get current indexing threshold
	targetCollectionInfo, err := targetClient.GetCollectionInfo(ctx, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get target collection information: %w", err)
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
		return fmt.Errorf("failed disable indexing in target collection %w", err)
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
		return fmt.Errorf("failed creating index on source collection %w", err)
	}

	sourceNonMigratedPointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: sourceCollection,
		Exact:          &exactPointCount,
		Filter: &qdrant.Filter{
			MustNot: []*qdrant.Condition{
				qdrant.NewMatchKeyword("migrationMarker", migrationMarker),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	limit := r.BatchSize

	var offset *qdrant.PointId

	pterm.DefaultSection.Println("Starting data migration")

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourceNonMigratedPointCount)).Start()

	_ = pterm.DefaultTable.WithHasHeader().WithData(pterm.TableData{
		{"", "Type", "Host", "Collection", "Points"},
		{"Source", sourceType, sourceHost, sourceCollection, strconv.FormatUint(sourcePointCount, 10)},
		{"Target", sourceType, targetHost, targetCollection, strconv.FormatUint(sourceNonMigratedPointCount, 10)},
	}).Render()

	pterm.Info.Printfln("The migration marker is %s. To resume the migration, add this marker to the command.\n", migrationMarker)

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
			sourcePointCount, err = sourceClient.Count(ctx, &qdrant.CountPoints{
				CollectionName: sourceCollection,
				Exact:          &exactPointCount,
			})
			if err != nil {
				return fmt.Errorf("failed to count points in source: %w", err)
			}
			bar.Total = int(sourcePointCount)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: targetCollection,
		Exact:          &exactPointCount,
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	// reset indexing threshold to enable indexing again
	err = targetClient.UpdateCollection(ctx, &qdrant.UpdateCollection{
		CollectionName: targetCollection,
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
