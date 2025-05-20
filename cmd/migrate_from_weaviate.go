package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pterm/pterm"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/auth"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromWeaviateCmd struct {
	Weaviate    commons.WeaviateConfig  `embed:"" prefix:"weaviate."`
	Qdrant      commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration   commons.MigrationConfig `embed:"" prefix:"migration."`
	DenseVector string                  `prefix:"qdrant." help:"Name of the dense vector in Qdrant" default:"dense_vector"`
	Distance    string                  `prefix:"qdrant." enum:"cosine,dot,euclid,manhattan" help:"Distance metric for the Qdrant collection" default:"cosine"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromWeaviateCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromWeaviateCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromWeaviateCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Weaviate to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := r.connectToWeaviate()
	if err != nil {
		return fmt.Errorf("failed to connect to Weaviate source: %w", err)
	}

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countWeaviateObjects(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to count objects in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceClient, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("weaviate", r.Weaviate.ClassName, r.Qdrant.Collection)

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

	return nil
}

func (r *MigrateFromWeaviateCmd) parseWeaviateOptions() (weaviate.Config, error) {
	cfg := weaviate.Config{
		Host:   r.Weaviate.Host,
		Scheme: r.Weaviate.Scheme,
	}

	switch r.Weaviate.AuthType {
	case "apiKey":
		if r.Weaviate.APIKey == "" {
			return cfg, errors.New("API key is required for apiKey authentication")
		}
		cfg.AuthConfig = auth.ApiKey{
			Value: r.Weaviate.APIKey,
		}
	case "password":
		if r.Weaviate.Username == "" || r.Weaviate.Password == "" {
			return cfg, errors.New("username and password are required for password authentication")
		}
		cfg.AuthConfig = auth.ResourceOwnerPasswordFlow{
			Username: r.Weaviate.Username,
			Password: r.Weaviate.Password,
			Scopes:   r.Weaviate.Scopes,
		}
	case "client":
		if r.Weaviate.ClientSecret == "" {
			return cfg, errors.New("client secret is required for client credentials authentication")
		}
		cfg.AuthConfig = auth.ClientCredentials{
			ClientSecret: r.Weaviate.ClientSecret,
		}
	case "bearer":
		if r.Weaviate.Token == "" {
			return cfg, errors.New("token is required for bearer token authentication")
		}
		cfg.AuthConfig = auth.BearerToken{
			AccessToken: r.Weaviate.Token,
		}
	}

	return cfg, nil
}

func (r *MigrateFromWeaviateCmd) connectToWeaviate() (*weaviate.Client, error) {
	cfg, err := r.parseWeaviateOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to parse Weaviate options: %w", err)
	}

	client, err := weaviate.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Weaviate client: %w", err)
	}

	return client, nil
}

func (r *MigrateFromWeaviateCmd) getClassSchema(ctx context.Context, client *weaviate.Client) (*models.Class, error) {
	schema, err := client.Schema().Getter().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	for _, class := range schema.Classes {
		if class.Class == r.Weaviate.ClassName {
			return class, nil
		}
	}

	return nil, fmt.Errorf("class %s not found", r.Weaviate.ClassName)
}

func (r *MigrateFromWeaviateCmd) getWeaviateVectorDimension(ctx context.Context, client *weaviate.Client) (int, error) {
	// Sadly, we can't get the dimensions from the collection info
	// https://forum.weaviate.io/t/get-vector-dimension-of-a-collection/1769/2?u=serendipitous
	// So we'll have to fetch one object to do so.
	result, err := client.GraphQL().Get().
		WithClassName(r.Weaviate.ClassName).
		WithLimit(1).
		WithFields(graphql.Field{
			Name: "_additional",
			Fields: []graphql.Field{
				{Name: "vector"},
			},
		}).
		Do(ctx)

	if err != nil {
		return 0, fmt.Errorf("failed to get object from Weaviate: %w", err)
	}

	getData, ok := result.Data["Get"].(map[string]any)
	if !ok {
		return 0, errors.New("invalid response format from Weaviate")
	}

	classData, ok := getData[r.Weaviate.ClassName].([]any)
	if !ok || len(classData) == 0 {
		return 0, errors.New("no objects found in class")
	}

	firstObject, ok := classData[0].(map[string]any)
	if !ok {
		return 0, errors.New("invalid object format")
	}

	additional, ok := firstObject["_additional"].(map[string]any)
	if !ok {
		return 0, errors.New("_additional field not found in object")
	}

	vector, ok := additional["vector"].([]any)
	if !ok {
		return 0, errors.New("vector field not found in _additional")
	}

	return len(vector), nil
}

func (r *MigrateFromWeaviateCmd) countWeaviateObjects(ctx context.Context, client *weaviate.Client) (uint64, error) {
	result, err := client.GraphQL().Aggregate().
		WithClassName(r.Weaviate.ClassName).
		WithFields(graphql.Field{
			Name: "meta",
			Fields: []graphql.Field{
				{Name: "count"},
			},
		}).
		Do(ctx)

	if err != nil {
		return 0, fmt.Errorf("failed to count objects in Weaviate: %w", err)
	}

	aggregateData, ok := result.Data["Aggregate"].(map[string]any)
	if !ok {
		return 0, errors.New("invalid response format from Weaviate aggregate")
	}

	classData, ok := aggregateData[r.Weaviate.ClassName].([]any)
	if !ok || len(classData) == 0 {
		return 0, errors.New("no aggregate data found for class")
	}

	metaData, ok := classData[0].(map[string]any)["meta"].(map[string]any)
	if !ok {
		return 0, errors.New("meta field not found in aggregate data")
	}

	count, ok := metaData["count"].(float64)
	if !ok {
		return 0, errors.New("count field not found in meta data")
	}

	return uint64(count), nil
}

func (r *MigrateFromWeaviateCmd) prepareTargetCollection(ctx context.Context, sourceClient *weaviate.Client, targetClient *qdrant.Client) error {
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

	vectorDimension, err := r.getWeaviateVectorDimension(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to get vector dimension: %w", err)
	}

	distanceMapping := map[string]qdrant.Distance{
		"euclid":    qdrant.Distance_Euclid,
		"cosine":    qdrant.Distance_Cosine,
		"dot":       qdrant.Distance_Dot,
		"manhattan": qdrant.Distance_Manhattan,
	}

	createReq := &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
			r.DenseVector: {
				Size:     uint64(vectorDimension),
				Distance: distanceMapping[r.Distance],
			},
		}),
	}

	if err := targetClient.CreateCollection(ctx, createReq); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection '%s' with dimension %d", r.Qdrant.Collection, vectorDimension)
	return nil
}

func (r *MigrateFromWeaviateCmd) migrateData(ctx context.Context, sourceClient *weaviate.Client, targetClient *qdrant.Client, sourcePointCount uint64) error {
	var offsetCount uint64
	var offsetID *qdrant.PointId

	if !r.Migration.Restart {
		offsetIDStored, offsetCountStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Weaviate.ClassName)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = offsetCountStored
		offsetID = offsetIDStored
	}

	classSchema, err := r.getClassSchema(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to get class schema: %w", err)
	}

	var fields []graphql.Field
	for _, prop := range classSchema.Properties {
		fields = append(fields, graphql.Field{Name: prop.Name})
	}

	fields = append(fields, graphql.Field{
		Name: "_additional",
		Fields: []graphql.Field{
			{Name: "id"},
			{Name: "vector"},
		},
	})

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		query := sourceClient.GraphQL().Get().
			WithClassName(r.Weaviate.ClassName).
			WithLimit(r.Migration.BatchSize).
			WithFields(fields...).
			WithTenant(r.Weaviate.Tenant)

		if offsetID != nil {
			query = query.WithAfter(offsetID.GetUuid())
		}

		result, err := query.Do(ctx)
		if err != nil {
			return fmt.Errorf("failed to get objects from Weaviate: %w", err)
		}

		getData, ok := result.Data["Get"].(map[string]any)
		if !ok {
			return errors.New("invalid response format from Weaviate")
		}

		objects, ok := getData[r.Weaviate.ClassName].([]any)
		if !ok {
			return errors.New("class data not found in response")
		}

		count := len(objects)
		if count == 0 {
			break
		}

		targetPoints := make([]*qdrant.PointStruct, 0, count)

		for _, obj := range objects {
			objMap, ok := obj.(map[string]any)
			if !ok {
				return errors.New("invalid object format")
			}

			additional, ok := objMap["_additional"].(map[string]any)
			if !ok {
				return errors.New("missing _additional field")
			}

			id, ok := additional["id"].(string)
			if !ok {
				return errors.New("missing id field")
			}

			rawVector, ok := additional["vector"].([]any)
			if !ok {
				return errors.New("missing vector field")
			}

			vector := make([]float32, len(rawVector))
			for i, val := range rawVector {
				if f, ok := val.(float64); ok {
					vector[i] = float32(f)
				} else {
					return errors.New("invalid vector format")
				}
			}

			cleanObj := make(map[string]any)
			for k, v := range objMap {
				if k != "_additional" {
					cleanObj[k] = v
				}
			}
			payload, err := qdrant.TryValueMap(cleanObj)
			if err != nil {
				return fmt.Errorf("failed to convert object to Qdrant payload: %w", err)
			}

			point := &qdrant.PointStruct{
				Id: qdrant.NewID(id),
				Vectors: qdrant.NewVectorsMap(map[string]*qdrant.Vector{
					r.DenseVector: qdrant.NewVectorDense(vector),
				}),
				Payload: payload,
			}

			targetPoints = append(targetPoints, point)
			offsetID = point.Id
		}

		_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return fmt.Errorf("failed to insert data into target: %w", err)
		}

		offsetCount += uint64(count)
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Weaviate.ClassName, offsetID, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(count)
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}
