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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type dynamoDBSource struct {
	keySchema          []types.KeySchemaElement
	vectorAttribute    string
	partitionAttribute string
	dimensions         int64
	distance           qdrant.Distance
}

type MigrateFromDynamoDBCmd struct {
	DynamoDB   commons.DynamoDBConfig  `embed:"" prefix:"dynamodb."`
	Qdrant     commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration  commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField    string                  `prefix:"qdrant." help:"Field storing the canonical DynamoDB primary key in Qdrant." default:"__id__"`
	VectorName string                  `prefix:"qdrant." help:"Target Qdrant vector name. Empty uses the unnamed vector."`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromDynamoDBCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}
	return nil
}

func (r *MigrateFromDynamoDBCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromDynamoDBCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("DynamoDB to Qdrant Data Migration")

	if err := r.Parse(); err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	sourceClient := dynamodb.NewFromConfig(sdkConfig)

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	if err := commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient); err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	source, err := r.describeSource(ctx, sourceClient)
	if err != nil {
		return err
	}
	if err := r.prepareTargetCollection(ctx, source, targetClient); err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("dynamodb", fmt.Sprintf("%s/%s", r.DynamoDB.Table, r.DynamoDB.Index), r.Qdrant.Collection)

	if err := r.migrateData(ctx, sourceClient, targetClient, source); err != nil {
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

	if err := commons.DeleteOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient); err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}
	return nil
}

func (r *MigrateFromDynamoDBCmd) describeSource(ctx context.Context, client *dynamodb.Client) (*dynamoDBSource, error) {
	output, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &r.DynamoDB.Table})
	if err != nil {
		return nil, fmt.Errorf("failed to describe DynamoDB table: %w", err)
	}
	if output.Table == nil {
		return nil, fmt.Errorf("DynamoDB returned no description for table %q", r.DynamoDB.Table)
	}

	for _, index := range output.Table.VectorIndexes {
		if index.IndexName == nil || *index.IndexName != r.DynamoDB.Index {
			continue
		}
		if index.IndexStatus != types.IndexStatusActive {
			return nil, fmt.Errorf("DynamoDB vector index %q is not active (status: %s)", r.DynamoDB.Index, index.IndexStatus)
		}
		if index.VectorAttribute == nil || index.VectorAttribute.AttributeName == nil || index.Dimensions == nil {
			return nil, fmt.Errorf("DynamoDB vector index %q has incomplete metadata", r.DynamoDB.Index)
		}

		distance, ok := map[types.VectorDistanceFunction]qdrant.Distance{
			types.VectorDistanceFunctionCosine:     qdrant.Distance_Cosine,
			types.VectorDistanceFunctionEuclidean:  qdrant.Distance_Euclid,
			types.VectorDistanceFunctionDotProduct: qdrant.Distance_Dot,
		}[index.DistanceFunction]
		if !ok {
			return nil, fmt.Errorf("unsupported DynamoDB distance function: %s", index.DistanceFunction)
		}

		source := &dynamoDBSource{
			keySchema:       output.Table.KeySchema,
			vectorAttribute: *index.VectorAttribute.AttributeName,
			dimensions:      *index.Dimensions,
			distance:        distance,
		}
		for _, element := range index.SearchSchema {
			if element.SearchSchemaElementType == types.SearchSchemaElementTypeHash && element.AttributeName != nil {
				source.partitionAttribute = *element.AttributeName
				break
			}
		}
		return source, nil
	}

	return nil, fmt.Errorf("vector index %q not found on DynamoDB table %q", r.DynamoDB.Index, r.DynamoDB.Table)
}

func (r *MigrateFromDynamoDBCmd) prepareTargetCollection(ctx context.Context, source *dynamoDBSource, targetClient *qdrant.Client) error {
	if !r.Migration.CreateCollection {
		return nil
	}
	exists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if exists {
		pterm.Info.Printfln("Target collection %q already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}

	params := &qdrant.VectorParams{Size: uint64(source.dimensions), Distance: source.distance}
	vectorsConfig := qdrant.NewVectorsConfig(params)
	if r.VectorName != "" {
		vectorsConfig = qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{r.VectorName: params})
	}
	if err := targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig:  vectorsConfig,
	}); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}
	pterm.Success.Printfln("Created target collection %q", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromDynamoDBCmd) migrateData(ctx context.Context, sourceClient *dynamodb.Client, targetClient *qdrant.Client, source *dynamoDBSource) error {
	offsetKey := fmt.Sprintf("%s/%s", r.DynamoDB.Table, r.DynamoDB.Index)
	var startKey map[string]types.AttributeValue
	offsetCount := uint64(0)

	if !r.Migration.Restart {
		offsetID, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
		if offsetID != nil {
			startKey, err = attributevalue.UnmarshalMapJSON([]byte(offsetID.GetUuid()))
			if err != nil {
				return fmt.Errorf("failed to decode start key: %w", err)
			}
		}
	}
	for {
		limit := int32(r.Migration.BatchSize)
		result, err := sourceClient.Scan(ctx, &dynamodb.ScanInput{
			TableName:         &r.DynamoDB.Table,
			Limit:             &limit,
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return fmt.Errorf("failed to scan DynamoDB: %w", err)
		}

		points := make([]*qdrant.PointStruct, 0, len(result.Items))
		for _, item := range result.Items {
			if _, ok := item[source.vectorAttribute]; !ok {
				continue
			}
			if source.partitionAttribute != "" {
				if _, ok := item[source.partitionAttribute]; !ok {
					continue
				}
			}
			point, err := r.dynamoDBItemToPoint(item, source)
			if err != nil {
				return err
			}
			points = append(points, point)
		}
		if len(points) > 0 {
			if err := upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
				CollectionName: r.Qdrant.Collection,
				Points:         points,
				Wait:           qdrant.PtrOf(true),
			}); err != nil {
				return err
			}
			offsetCount += uint64(len(points))
		}

		if len(result.LastEvaluatedKey) == 0 {
			pterm.Success.Printfln("Data migration finished successfully")
			return nil
		}
		checkpoint, err := marshalDynamoDBKey(result.LastEvaluatedKey)
		if err != nil {
			return fmt.Errorf("failed to encode checkpoint: %w", err)
		}
		if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey, qdrant.NewID(string(checkpoint)), offsetCount); err != nil {
			return fmt.Errorf("failed to store checkpoint: %w", err)
		}

		startKey = result.LastEvaluatedKey
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}
}

func (r *MigrateFromDynamoDBCmd) dynamoDBItemToPoint(item map[string]types.AttributeValue, source *dynamoDBSource) (*qdrant.PointStruct, error) {
	vector, err := dynamoDBVector(item[source.vectorAttribute])
	if err != nil {
		return nil, fmt.Errorf("invalid vector attribute %q: %w", source.vectorAttribute, err)
	}
	canonicalKey, err := canonicalDynamoDBKey(item, source.keySchema)
	if err != nil {
		return nil, err
	}

	payload, err := dynamoDBPayload(item, source.vectorAttribute)
	if err != nil {
		return nil, fmt.Errorf("failed to convert DynamoDB payload: %w", err)
	}
	payload[r.IdField] = canonicalKey

	point := &qdrant.PointStruct{
		Id:      arbitraryIDToUUID(canonicalKey),
		Payload: qdrant.NewValueMap(payload),
		Vectors: qdrant.NewVectorsDense(vector),
	}
	if r.VectorName != "" {
		point.Vectors = qdrant.NewVectorsMap(map[string]*qdrant.Vector{
			r.VectorName: qdrant.NewVectorDense(vector),
		})
	}
	return point, nil
}

func dynamoDBVector(value types.AttributeValue) ([]float32, error) {
	list, ok := value.(*types.AttributeValueMemberL)
	if !ok {
		return nil, fmt.Errorf("expected a list of numbers, got %T", value)
	}
	vector := make([]float32, len(list.Value))
	for i, element := range list.Value {
		number, ok := element.(*types.AttributeValueMemberN)
		if !ok {
			return nil, fmt.Errorf("element %d is %T, expected a number", i, element)
		}
		parsed, err := strconv.ParseFloat(number.Value, 32)
		if err != nil {
			return nil, fmt.Errorf("element %d has invalid number %q: %w", i, number.Value, err)
		}
		vector[i] = float32(parsed)
	}
	return vector, nil
}

func canonicalDynamoDBKey(item map[string]types.AttributeValue, schema []types.KeySchemaElement) (string, error) {
	key := make(map[string]types.AttributeValue, len(schema))
	for _, element := range schema {
		if element.AttributeName == nil {
			return "", fmt.Errorf("DynamoDB table returned a key schema with no attribute name")
		}
		value, ok := item[*element.AttributeName]
		if !ok {
			return "", fmt.Errorf("item is missing primary key attribute %q", *element.AttributeName)
		}
		key[*element.AttributeName] = value
	}
	data, err := marshalDynamoDBKey(key)
	if err != nil {
		return "", fmt.Errorf("failed to marshal primary key: %w", err)
	}
	return string(data), nil
}

func marshalDynamoDBKey(key map[string]types.AttributeValue) ([]byte, error) {
	data, err := attributevalue.MarshalMapJSON(key)
	if err != nil {
		return nil, err
	}

	// Re-marshal through encoding/json so map keys have a deterministic order.
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func dynamoDBPayload(item map[string]types.AttributeValue, vectorAttribute string) (map[string]any, error) {
	attributes := make(map[string]types.AttributeValue, len(item))
	for name, value := range item {
		if name != vectorAttribute {
			attributes[name] = value
		}
	}

	var decoded map[string]any
	if err := attributevalue.UnmarshalMap(attributes, &decoded); err != nil {
		return nil, err
	}

	// Normalize typed sets and byte slices into JSON-compatible values accepted by Qdrant.
	data, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return payload, nil
}
