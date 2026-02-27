package integrationtests

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/qdrant/go-client/qdrant"
)

func TestMigrateFromWeaviate(t *testing.T) {
	ctx := context.Background()

	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	weaviateCont := weaviateContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, weaviateCont.Terminate(ctx))
	})

	var err error
	weaviateHost, err := weaviateCont.PortEndpoint(ctx, "8080/tcp", "")
	require.NoError(t, err)

	qdrantHost, err := qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort := mappedPort

	weaviateClient, err := weaviate.NewClient(weaviate.Config{
		Host:   weaviateHost,
		Scheme: "http",
	})
	require.NoError(t, err)

	class := &models.Class{
		Class:      testCollectionName,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "content",
				DataType: []string{"text"},
			},
			{
				Name:     "genre",
				DataType: []string{"text"},
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "dot",
		},
		VectorIndexType: "hnsw",
	}

	err = weaviateClient.Schema().ClassCreator().WithClass(class).Do(ctx)
	require.NoError(t, err)

	testIDs, vectors := createWeaviateTestData()
	for i, vec := range vectors {
		obj := map[string]interface{}{
			"content": fmt.Sprintf("test content %d", i+1),
			"genre":   fmt.Sprintf("test genre %d", i+1),
		}

		_, err = weaviateClient.Data().Creator().
			WithClassName(testCollectionName).
			WithID(testIDs[i]).
			WithProperties(obj).
			WithVector(vec).
			Do(ctx)
		require.NoError(t, err)
	}

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   qdrantPort.Int(),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	err = qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: testCollectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(dimension),
			Distance: qdrant.Distance_Dot,
		}),
	})
	require.NoError(t, err)

	args := []string{
		"weaviate",
		fmt.Sprintf("--weaviate.host=%s", weaviateHost),
		fmt.Sprintf("--weaviate.class-name=%s", testCollectionName),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
	}

	runMigrationBinary(t, args)

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(len(testIDs))),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(testIDs))

	expectedPoints := make(map[string]struct {
		content string
		genre   string
		vector  []float32
	})
	for i, id := range testIDs {
		expectedPoints[id] = struct {
			content string
			genre   string
			vector  []float32
		}{
			content: fmt.Sprintf("test content %d", i+1),
			genre:   fmt.Sprintf("test genre %d", i+1),
			vector:  vectors[i],
		}
	}

	for _, point := range points {
		id := point.Id.GetUuid()
		expected, exists := expectedPoints[id]
		require.True(t, exists)

		require.Equal(t, expected.content, point.Payload["content"].GetStringValue())
		require.Equal(t, expected.genre, point.Payload["genre"].GetStringValue())

		vector := point.Vectors.GetVector().GetDenseVector().GetData()
		require.Equal(t, expected.vector, vector)
	}
}

func createWeaviateTestData() ([]string, [][]float32) {
	ids := make([]string, totalEntries)
	vectors := make([][]float32, totalEntries)

	for i := 0; i < totalEntries; i++ {
		ids[i] = uuid.New().String()
		vectors[i] = randFloat32Values(dimension)
	}
	return ids, vectors
}
