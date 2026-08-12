package integrationtests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const (
	dynamoDBVectorName = "dense_vector"
)

func TestMigrateFromDynamoDB(t *testing.T) {
	ctx := context.Background()
	tableName := os.Getenv("AWS_DYNAMODB_TABLE")
	if tableName == "" {
		t.Skip("Skipping DynamoDB source test. AWS_DYNAMODB_TABLE not set")
	}
	indexName := os.Getenv("AWS_DYNAMODB_VECTOR_INDEX")
	if indexName == "" {
		t.Skip("Skipping DynamoDB source test. AWS_DYNAMODB_VECTOR_INDEX not set")
	}
	testID := fmt.Sprintf("dynamodb-test-%d", rand.Int())
	collectionName := testCollectionName + "-" + testID

	sdkConfig, err := config.LoadDefaultConfig(ctx)
	require.NoError(t, err)
	dynamoClient := dynamodb.NewFromConfig(sdkConfig)

	expected := make(map[string]struct {
		age    int64
		name   string
		city   string
		vector []float32
	}, totalEntries)
	writes := make([]types.WriteRequest, 0, totalEntries+1)
	deletes := make([]types.WriteRequest, 0, totalEntries+1)
	for i := 0; i < totalEntries; i++ {
		id := fmt.Sprintf("%s-vector-%d", testID, i)
		vector := randFloat32Values(dimension)
		vectorValue := make([]types.AttributeValue, dimension)
		for j, value := range vector {
			vectorValue[j] = &types.AttributeValueMemberN{
				Value: strconv.FormatFloat(float64(value), 'g', -1, 32),
			}
		}
		expected[id] = struct {
			age    int64
			name   string
			city   string
			vector []float32
		}{
			age:    int64(i),
			name:   fmt.Sprintf("name-%d", i),
			city:   fmt.Sprintf("city-%d", i),
			vector: normalizedVector(vector),
		}
		writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
			"user_id":          &types.AttributeValueMemberS{Value: id},
			dynamoDBVectorName: &types.AttributeValueMemberL{Value: vectorValue},
			"age":              &types.AttributeValueMemberN{Value: strconv.Itoa(i)},
			"name":             &types.AttributeValueMemberS{Value: fmt.Sprintf("name-%d", i)},
			"city":             &types.AttributeValueMemberS{Value: fmt.Sprintf("city-%d", i)},
		}}})
		deletes = append(deletes, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
			"user_id": &types.AttributeValueMemberS{Value: id},
		}}})
	}
	nonVectorID := testID + "-without-vector"
	writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
		"user_id": &types.AttributeValueMemberS{Value: nonVectorID},
		"name":    &types.AttributeValueMemberS{Value: "not migrated"},
	}}})
	deletes = append(deletes, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
		"user_id": &types.AttributeValueMemberS{Value: nonVectorID},
	}}})
	t.Cleanup(func() {
		batchWriteDynamoDBItems(t, dynamoClient, tableName, deletes)
	})
	batchWriteDynamoDBItems(t, dynamoClient, tableName, writes)

	qdrantContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		require.NoError(t, qdrantContainer.Terminate(ctx))
	}()
	qdrantHost, err := qdrantContainer.Host(ctx)
	require.NoError(t, err)
	qdrantPort, err := qdrantContainer.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)

	args := []string{
		"dynamodb",
		fmt.Sprintf("--dynamodb.table=%s", tableName),
		fmt.Sprintf("--dynamodb.index=%s", indexName),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
		fmt.Sprintf("--qdrant.collection=%s", collectionName),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		fmt.Sprintf("--qdrant.vector-name=%s", dynamoDBVectorName),
		"--migration.batch-size=10",
	}
	runMigrationBinary(t, args)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   int(qdrantPort.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: collectionName,
		Limit:          qdrant.PtrOf(uint32(10_000)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)

	migratedFixtures := 0
	for _, point := range points {
		id := point.Payload["user_id"].GetStringValue()
		require.NotEqual(t, nonVectorID, id)
		exp, ok := expected[id]
		if !ok {
			continue
		}
		migratedFixtures++
		require.Equal(t, exp.name, point.Payload["name"].GetStringValue())
		require.Equal(t, exp.city, point.Payload["city"].GetStringValue())
		require.InDelta(t, float64(exp.age), point.Payload["age"].GetDoubleValue(), 0)
		require.JSONEq(t, fmt.Sprintf(`{"user_id":{"S":%q}}`, id), point.Payload[idField].GetStringValue())
		vector := point.Vectors.GetVectors().GetVectors()[dynamoDBVectorName].GetDenseVector().GetData()
		require.InDeltaSlice(t, exp.vector, vector, 1e-6)
	}
	require.Equal(t, totalEntries, migratedFixtures)
}

func normalizedVector(vector []float32) []float32 {
	var squaredSum float64
	for _, value := range vector {
		squaredSum += float64(value * value)
	}
	norm := float32(math.Sqrt(squaredSum))
	result := make([]float32, len(vector))
	for i, value := range vector {
		result[i] = value / norm
	}
	return result
}

func batchWriteDynamoDBItems(t *testing.T, client *dynamodb.Client, tableName string, writes []types.WriteRequest) {
	t.Helper()
	for len(writes) > 0 {
		batchSize := min(25, len(writes))
		result, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{tableName: writes[:batchSize]},
		})
		require.NoError(t, err)
		require.Empty(t, result.UnprocessedItems)
		writes = writes[batchSize:]
	}
}
