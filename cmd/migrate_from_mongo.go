package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pterm/pterm"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromMongoDBCmd struct {
	MongoDB      commons.MongoDBConfig   `embed:"" prefix:"mongodb."`
	Qdrant       commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration    commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField      string                  `prefix:"qdrant." help:"Field storing MongoDB IDs in Qdrant." default:"__id__"`
	VectorFields []string                `help:"Names of fields containing vector data"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromMongoDBCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromMongoDBCmd) Validate() error {
	if len(r.VectorFields) == 0 {
		return fmt.Errorf("at least one vector field must be specified")
	}

	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromMongoDBCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("MongoDB to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := r.connectToMongoDB()
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB source: %w", err)
	}
	defer sourceClient.Disconnect(ctx)

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countMongoDBDocuments(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to count documents in source: %w", err)
	}

	displayMigrationStart("mongodb", r.MongoDB.Collection, r.Qdrant.Collection)

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

func (r *MigrateFromMongoDBCmd) connectToMongoDB() (*mongo.Client, error) {
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(r.MongoDB.Url).SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB client: %w", err)
	}

	return client, nil
}

func (r *MigrateFromMongoDBCmd) countMongoDBDocuments(ctx context.Context, client *mongo.Client) (int64, error) {
	collection := client.Database(r.MongoDB.Database).Collection(r.MongoDB.Collection)

	count, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	return count, nil
}

func (r *MigrateFromMongoDBCmd) migrateData(ctx context.Context, sourceClient *mongo.Client, targetClient *qdrant.Client, sourcePointCount int64) error {
	batchSize := uint64(r.Migration.BatchSize)
	collection := sourceClient.Database(r.MongoDB.Database).Collection(r.MongoDB.Collection)

	page := uint64(0)
	offsetCount := uint64(0)

	if !r.Migration.Restart {
		_, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.MongoDB.Collection)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
		page = uint64(offsetCount / batchSize)
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, uint64(offsetCount))

	for {
		skip := page * batchSize
		findOptions := options.Find().
			SetLimit(int64(batchSize)).
			SetSkip(int64(skip))

		cursor, err := collection.Find(ctx, map[string]any{}, findOptions)
		if err != nil {
			return fmt.Errorf("failed to query MongoDB: %w", err)
		}

		var results []bson.M
		if err = cursor.All(ctx, &results); err != nil {
			cursor.Close(ctx)
			return fmt.Errorf("failed to decode results: %w", err)
		}
		cursor.Close(ctx)

		if len(results) == 0 {
			break
		}

		var targetPoints []*qdrant.PointStruct
		for _, doc := range results {
			point := &qdrant.PointStruct{}
			vectors := make(map[string]*qdrant.Vector)
			payload := make(map[string]interface{})

			id_str := doc["_id"].(primitive.ObjectID).String()
			point.Id = arbitraryIDToUUID(id_str)
			payload[r.IdField] = qdrant.NewValueString(id_str)

			for fieldName, value := range doc {
				if fieldName == "_id" {
					continue
				}

				isVectorField := false
				for _, vf := range r.VectorFields {
					if fieldName == vf {
						isVectorField = true
						break
					}
				}

				if isVectorField {
					vector, err := r.extractVectorFromValue(value)
					if err != nil {
						return fmt.Errorf("failed to extract vector from field %s: %w", fieldName, err)
					}
					if vector != nil {
						vectors[fieldName] = qdrant.NewVector(vector...)
					}
				} else {
					payload[fieldName] = value
				}
			}

			if len(vectors) > 0 {
				point.Vectors = qdrant.NewVectorsMap(vectors)
			}
			point.Payload = qdrant.NewValueMap(payload)
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
		offsetId := qdrant.NewIDNum(0)
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.MongoDB.Collection, offsetId, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))
		page++
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}
func (r *MigrateFromMongoDBCmd) extractVectorFromValue(value interface{}) ([]float32, error) {
	var items []interface{}

	switch v := value.(type) {
	case []interface{}:
		items = v
	case []float32:
		return v, nil
	case []float64:
		items = make([]interface{}, len(v))
		for i, f := range v {
			items[i] = f
		}
	case primitive.A:
		items = []interface{}(v)
	default:
		return nil, fmt.Errorf("unsupported vector type: %T", value)
	}

	vector := make([]float32, len(items))
	for i, item := range items {
		f, err := toFloat32(item)
		if err != nil {
			return nil, fmt.Errorf("item at index %d: %w", i, err)
		}
		vector[i] = f
	}

	return vector, nil
}

func toFloat32(value interface{}) (float32, error) {
	switch v := value.(type) {
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	case int:
		return float32(v), nil
	case int32:
		return float32(v), nil
	case int64:
		return float32(v), nil
	default:
		return 0, fmt.Errorf("unsupported number type: %T", v)
	}
}
