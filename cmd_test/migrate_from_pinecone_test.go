package cmd_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pinecone-io/go-pinecone/v3/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/cmd"
	"github.com/qdrant/migration/pkg/commons"
)

func TestMigrateFromPinecone(t *testing.T) {
	ctx := context.Background()

	qdrantContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		if err := qdrantContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Qdrant container: %v", err)
		}
	}()

	pineconeContainer := pineconeContainer(ctx, t)
	defer func() {
		if err := pineconeContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Pinecone container: %v", err)
		}
	}()

	pineconeHost, err := pineconeContainer.PortEndpoint(context.Background(), "5081/tcp", "http")
	require.NoError(t, err)
	pineconeIndexHost, err := pineconeContainer.PortEndpoint(context.Background(), "5082/tcp", "http")
	require.NoError(t, err)

	qdrantHost, err := qdrantContainer.Host(ctx)
	require.NoError(t, err)
	qdrantPort, err := qdrantContainer.MappedPort(ctx, qdrantPort)
	require.NoError(t, err)

	pineconeClient, err := pinecone.NewClient(pinecone.NewClientParams{
		Host:   pineconeHost,
		ApiKey: "qdrant-migration-test",
	})
	require.NoError(t, err)

	metric := pinecone.Euclidean
	dims := int32(dimension)

	indexName := "my-serverless-index"

	_, err = pineconeClient.CreateServerlessIndex(ctx, &pinecone.CreateServerlessIndexRequest{
		Name:      indexName,
		Dimension: &dims,
		Metric:    &metric,
		Cloud:     pinecone.Aws,
		Region:    "us-east-1",
	})
	require.NoError(t, err)

	indexConn, err := pineconeClient.Index(pinecone.NewIndexConnParams{
		Host: pineconeIndexHost,
	})
	require.NoError(t, err)

	testIDs := make([]string, totalEntries)
	testVectors := make([][]float32, totalEntries)
	testMetadatas := make([]map[string]interface{}, totalEntries)

	for i := 0; i < totalEntries; i++ {
		testIDs[i] = fmt.Sprintf("%d", i+1)

		randomVector := make([]float32, dimension)
		for j := range randomVector {
			randomVector[j] = rand.Float32()
		}
		testVectors[i] = randomVector

		testMetadatas[i] = map[string]interface{}{
			"source": fmt.Sprintf("test%d", i+1),
		}
	}

	vectors := make([]*pinecone.Vector, totalEntries)
	for i := range testIDs {
		metadata, err := structpb.NewStruct(testMetadatas[i])
		require.NoError(t, err)
		vectors[i] = &pinecone.Vector{
			Id:       testIDs[i],
			Values:   &testVectors[i],
			Metadata: metadata,
		}
	}

	_, err = indexConn.UpsertVectors(ctx, vectors)
	require.NoError(t, err)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:   qdrantHost,
		Port:   qdrantPort.Int(),
		APIKey: qdrantAPIKey,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	migrationCmd := &cmd.MigrateFromPineconeCmd{
		Pinecone: commons.PineconeConfig{
			IndexName:   indexName,
			IndexHost:   pineconeIndexHost,
			ServiceHost: pineconeHost,
			APIKey:      "qdrant-migration-test",
		},
		Qdrant: commons.QdrantConfig{
			Url:        "http://" + qdrantHost + ":" + qdrantPort.Port(),
			Collection: testCollectionName,
			APIKey:     qdrantAPIKey,
		},
		Migration: commons.MigrationConfig{
			BatchSize:            batchSize,
			CreateCollection:     true,
			EnsurePayloadIndexes: true,
			OffsetsCollection:    offsetsCollectionName,
		},
		IdField:      idField,
		DenseVector:  denseVectorName,
		SparseVector: sparseVectorName,
	}

	err = migrationCmd.Run(&cmd.Globals{})
	require.NoError(t, err)

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(len(testIDs))),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(testIDs))

	expectedPoints := make(map[string]struct {
		source string
		vector []float32
	})
	for i, id := range testIDs {
		expectedPoints[id] = struct {
			source string
			vector []float32
		}{
			source: testMetadatas[i]["source"].(string),
			vector: testVectors[i],
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		expected, exists := expectedPoints[id]
		require.True(t, exists)

		require.Equal(t, expected.source, point.Payload["source"].GetStringValue())

		vector := point.Vectors.GetVectors().GetVectors()[denseVectorName].GetData()
		require.Equal(t, expected.vector, vector)
	}
}
