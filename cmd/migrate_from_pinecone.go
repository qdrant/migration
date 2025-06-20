package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pinecone-io/go-pinecone/v3/pinecone"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromPineconeCmd struct {
	Pinecone     commons.PineconeConfig  `embed:"" prefix:"pinecone."`
	Qdrant       commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration    commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField      string                  `prefix:"qdrant." help:"Field storing Pinecone IDs in Qdrant." default:"__id__"`
	DenseVector  string                  `prefix:"qdrant." help:"Name of the dense vector in Qdrant" default:"dense_vector"`
	SparseVector string                  `prefix:"qdrant." help:"Name of the sparse vector in Qdrant" default:"sparse_vector"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromPineconeCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromPineconeCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromPineconeCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Pinecone to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, sourceIndexConn, err := r.connectToPinecone()
	if err != nil {
		return fmt.Errorf("failed to connect to Pinecone source: %w", err)
	}

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countPineconeVectors(ctx, sourceIndexConn)
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceClient, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("pinecone", r.Pinecone.IndexHost, r.Qdrant.Collection)

	err = r.migrateData(ctx, sourceIndexConn, targetClient, sourcePointCount)
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

func (r *MigrateFromPineconeCmd) connectToPinecone() (*pinecone.Client, *pinecone.IndexConnection, error) {
	client, err := pinecone.NewClient(pinecone.NewClientParams{
		Host:   r.Pinecone.ServiceHost,
		ApiKey: r.Pinecone.APIKey,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Pinecone client: %w", err)
	}

	indexConn, err := client.Index(pinecone.NewIndexConnParams{
		Host:      r.Pinecone.IndexHost,
		Namespace: r.Pinecone.Namespace,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to Pinecone index: %w", err)
	}

	return client, indexConn, nil
}

func (r *MigrateFromPineconeCmd) countPineconeVectors(ctx context.Context, indexConn *pinecone.IndexConnection) (uint64, error) {
	stats, err := indexConn.DescribeIndexStats(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get index statistics: %w", err)
	}

	return uint64(stats.TotalVectorCount), nil
}

func (r *MigrateFromPineconeCmd) prepareTargetCollection(ctx context.Context, sourceClient *pinecone.Client, targetClient *qdrant.Client) error {
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

	indexes, err := sourceClient.ListIndexes(ctx)
	if err != nil {
		return fmt.Errorf("failed to list Pinecone indexes: %w", err)
	}

	var foundIndex *pinecone.Index
	for i := range indexes {
		if indexes[i].Name == r.Pinecone.IndexName {
			foundIndex = indexes[i]
			break
		}
	}

	if foundIndex == nil {
		return fmt.Errorf("index %q not found in Pinecone", r.Pinecone.IndexName)
	}

	distanceMapping := map[pinecone.IndexMetric]qdrant.Distance{
		pinecone.Cosine:     qdrant.Distance_Cosine,
		pinecone.Euclidean:  qdrant.Distance_Euclid,
		pinecone.Dotproduct: qdrant.Distance_Dot,
	}

	var createReq *qdrant.CreateCollection

	switch foundIndex.VectorType {
	case "dense":
		createReq = &qdrant.CreateCollection{
			CollectionName: r.Qdrant.Collection,
			VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
				r.DenseVector: {
					Size:     uint64(*foundIndex.Dimension),
					Distance: distanceMapping[foundIndex.Metric],
				},
			}),
		}
	case "sparse":
		createReq = &qdrant.CreateCollection{
			CollectionName: r.Qdrant.Collection,
			SparseVectorsConfig: qdrant.NewSparseVectorsConfig(map[string]*qdrant.SparseVectorParams{
				r.SparseVector: {},
			}),
		}
	default:
		return fmt.Errorf("unsupported vector type: %s", foundIndex.VectorType)
	}

	if err := targetClient.CreateCollection(ctx, createReq); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection '%s'", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromPineconeCmd) migrateData(ctx context.Context, sourceIndexConn *pinecone.IndexConnection, targetClient *qdrant.Client, sourcePointCount uint64) error {
	batchSize := r.Migration.BatchSize

	var offsetId *qdrant.PointId
	offsetCount := uint64(0)

	if !r.Migration.Restart {
		id, offsetStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Pinecone.IndexHost)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = offsetStored
		offsetId = id
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		req := &pinecone.ListVectorsRequest{
			Limit: qdrant.PtrOf(uint32(batchSize)),
		}

		if offsetId != nil {
			req.PaginationToken = qdrant.PtrOf(offsetId.GetUuid())
		}

		listRes, err := sourceIndexConn.ListVectors(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to list vectors from Pinecone: %w", err)
		}

		if len(listRes.VectorIds) < 1 {
			break
		}

		ids := make([]string, 0, len(listRes.VectorIds))
		for _, id := range listRes.VectorIds {
			ids = append(ids, *id)
		}

		fetchRes, err := sourceIndexConn.FetchVectors(ctx, ids)
		if err != nil {
			return fmt.Errorf("failed to fetch vectors from Pinecone: %w", err)
		}

		var targetPoints []*qdrant.PointStruct
		for id, vec := range fetchRes.Vectors {
			point := &qdrant.PointStruct{
				// Pinecone allows arbitrary strings as ID.
				// Qdrant only allows UUIDs and +ve integers.
				// Ref: https://qdrant.tech/documentation/concepts/points/#point-ids
				// So we create a deterministic UUID based on the original ID.
				// A copy of the original ID is stored in the payload.
				Id: arbitraryIDToUUID(id),
			}
			vectorMap := make(map[string]*qdrant.Vector)

			if vec.Values != nil {
				vectorMap[r.DenseVector] = qdrant.NewVectorDense(*vec.Values)
			}

			if vec.SparseValues != nil {
				vectorMap[r.SparseVector] = qdrant.NewVectorSparse(vec.SparseValues.Indices, vec.SparseValues.Values)
			}

			if len(vectorMap) > 0 {
				point.Vectors = qdrant.NewVectorsMap(vectorMap)
			}

			payload := make(map[string]*qdrant.Value)
			if vec.Metadata != nil {
				payload = qdrant.NewValueMap(vec.Metadata.AsMap())
			}
			payload[r.IdField] = qdrant.NewValueString(id)
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

		if listRes.NextPaginationToken != nil {
			offsetCount += uint64(len(targetPoints))
			offsetId = qdrant.NewID(*listRes.NextPaginationToken)
			err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Pinecone.IndexHost, offsetId, offsetCount)
			if err != nil {
				return fmt.Errorf("failed to store offset: %w", err)
			}
		}

		bar.Add(len(targetPoints))

		if listRes.NextPaginationToken == nil {
			break
		}

	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}
