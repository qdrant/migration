package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	chroma "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromChromaCmd struct {
	Chroma        commons.ChromaConfig    `embed:"" prefix:"chroma."`
	Qdrant        commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration     commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField       string                  `prefix:"qdrant." help:"Field storing Chroma IDs in Qdrant." default:"__id__"`
	DenseVector   string                  `prefix:"qdrant." help:"Name of the dense vector in Qdrant" default:"dense_vector"`
	Distance      string                  `prefix:"qdrant." enum:"l2,cosine,ip" help:"Distance metric for the Qdrant collection" default:"l2"`
	DocumentField string                  `prefix:"qdrant." help:"Field storing Chroma documents in Qdrant." default:"document"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromChromaCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromChromaCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromChromaCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Chroma to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, sourceCollection, err := r.connectToChroma(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Chroma source: %w", err)
	}
	defer sourceCollection.Close()
	defer sourceClient.Close()

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countChromaVectors(ctx, sourceCollection)
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceCollection, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("chroma", *r.Chroma.Collection, r.Qdrant.Collection)

	err = r.migrateData(ctx, sourceCollection, targetClient, sourcePointCount)
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

func (r *MigrateFromChromaCmd) parseChromaOptions() ([]chroma.ClientOption, error) {
	clientOptions := []chroma.ClientOption{chroma.WithBaseURL(*r.Chroma.Url)}

	if r.Chroma.Database != nil && r.Chroma.Tenant != nil {
		clientOptions = append(clientOptions, chroma.WithDatabaseAndTenant(*r.Chroma.Database, *r.Chroma.Tenant))
	} else if r.Chroma.Tenant != nil {
		clientOptions = append(clientOptions, chroma.WithTenant(*r.Chroma.Tenant))
	}

	switch *r.Chroma.AuthType {
	case "basic":
		if r.Chroma.Username == nil || r.Chroma.Password == nil {
			return nil, errors.New("username and password are required for basic authentication")
		}
		authProvider := chroma.NewBasicAuthCredentialsProvider(*r.Chroma.Username, *r.Chroma.Password)
		clientOptions = append(clientOptions, chroma.WithAuth(authProvider))
	case "token":
		if r.Chroma.Token == nil {
			return nil, errors.New("token is required for token authentication")
		}
		authProvider := chroma.NewTokenAuthCredentialsProvider(*r.Chroma.Token, chroma.TokenTransportHeader(*r.Chroma.TokenHeader))
		clientOptions = append(clientOptions, chroma.WithAuth(authProvider))
	}

	return clientOptions, nil
}
func (r *MigrateFromChromaCmd) connectToChroma(ctx context.Context) (chroma.Client, chroma.Collection, error) {
	clientOptions, err := r.parseChromaOptions()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get parse Chroma options: %w", err)
	}

	client, err := chroma.NewHTTPClient(clientOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Chroma client: %w", err)
	}

	collection, err := client.GetOrCreateCollection(ctx, *r.Chroma.Collection)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get Chroma collection: %w", err)
	}

	return client, collection, nil
}

func (r *MigrateFromChromaCmd) countChromaVectors(ctx context.Context, collection chroma.Collection) (uint64, error) {
	count, err := collection.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get collection count: %w", err)
	}

	return uint64(count), nil
}

func (r *MigrateFromChromaCmd) prepareTargetCollection(ctx context.Context, collection chroma.Collection, targetClient *qdrant.Client) error {
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

	distanceMapping := map[string]qdrant.Distance{
		"l2":     qdrant.Distance_Euclid,
		"cosine": qdrant.Distance_Cosine,
		"ip":     qdrant.Distance_Dot,
	}

	createReq := &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
			r.DenseVector: {
				Size:     uint64(collection.Dimension()),
				Distance: distanceMapping[r.Distance],
			},
		}),
	}

	if err := targetClient.CreateCollection(ctx, createReq); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection '%s' with dimension", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromChromaCmd) migrateData(ctx context.Context, collection chroma.Collection, targetClient *qdrant.Client, sourcePointCount uint64) error {
	startTime := time.Now()
	batchSize := r.Migration.BatchSize

	var currentOffset uint64 = 0

	if !r.Migration.Restart {
		_, offsetStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, *r.Chroma.Collection)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		currentOffset = offsetStored
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, currentOffset)

	for {
		resp, err := collection.Get(
			ctx,
			chroma.WithLimitGet(int(batchSize)),
			chroma.WithOffsetGet(int(currentOffset)),
			chroma.WithIncludeGet("metadatas", "documents", "embeddings"),
		)
		if err != nil {
			return fmt.Errorf("failed to get vectors from Chroma: %w", err)
		}

		count := resp.Count()
		if count == 0 {
			break
		}

		targetPoints := make([]*qdrant.PointStruct, 0, count)
		ids := resp.GetIDs()
		embeddings := resp.GetEmbeddings()
		documents := resp.GetDocuments()

		// The Chroma Go client's metadata type, `chroma.DocumentMetadatas`` is restrictive.
		// So we convert it to a list of generic maps, `[]map[string]any``.
		// That is later parse into Qdrant payload with `qdrant.NewValueMap(...)``
		metadatas := resp.GetMetadatas()
		jsonData, err := json.Marshal(metadatas)
		if err != nil {
			log.Fatalf("Error marshaling metadata: %v", err)
		}
		var metadatasGeneric []map[string]any
		err = json.Unmarshal(jsonData, &metadatasGeneric)
		if err != nil {
			log.Fatalf("Error unmarshaling metadata: %v", err)
		}

		for i := 0; i < count; i++ {
			id := ids[i]
			embedding := embeddings[i]
			metadataValue := metadatasGeneric[i]

			point := &qdrant.PointStruct{
				Id: arbitraryIDToUUID(string(id)),
			}

			vectorMap := make(map[string]*qdrant.Vector)
			vectorMap[r.DenseVector] = qdrant.NewVectorDense(embedding.ContentAsFloat32())
			point.Vectors = qdrant.NewVectorsMap(vectorMap)

			payload := qdrant.NewValueMap(metadataValue)
			payload[r.IdField] = qdrant.NewValueString(string(id))

			if i < len(documents) && documents[i].ContentString() != "" {
				payload[r.DocumentField] = qdrant.NewValueString(documents[i].ContentString())
			}

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

		currentOffset += uint64(count)
		// Just a placeholder ID for offset tracking.
		// We're only using the offset count
		offsetId := qdrant.NewIDNum(0)
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, *r.Chroma.Collection, offsetId, currentOffset)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(count)

		// If one minute elapsed get updated sourcePointCount
		// Useful if any new points were added to the source during migration
		if time.Since(startTime) > time.Minute {
			sourcePointCount, err = r.countChromaVectors(ctx, collection)
			if err != nil {
				return fmt.Errorf("failed to count vectors in Chroma: %w", err)
			}
			bar.Total = int(sourcePointCount)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}
