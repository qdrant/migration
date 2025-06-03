package integrationtests

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

func TestMigrateFromRedis(t *testing.T) {
	ctx := context.Background()

	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	redisCont := redisContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, redisCont.Terminate(ctx))
	})

	var err error
	redisHost, err := redisCont.PortEndpoint(ctx, "6379/tcp", "")
	require.NoError(t, err)

	qdrantHost, err := qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantPort)
	require.NoError(t, err)
	qdrantPort := mappedPort.Int()

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		DB:       0,
		Protocol: 2,
	})
	defer rdb.Close()

	_, err = rdb.FTCreate(ctx,
		"vector_idx",
		&redis.FTCreateOptions{
			OnHash: true,
			Prefix: []any{"doc:"},
		},
		&redis.FieldSchema{
			FieldName: "content",
			FieldType: redis.SearchFieldTypeText,
		},
		&redis.FieldSchema{
			FieldName: "genre",
			FieldType: redis.SearchFieldTypeTag,
		},
		&redis.FieldSchema{
			FieldName: "embedding",
			FieldType: redis.SearchFieldTypeVector,
			VectorArgs: &redis.FTVectorArgs{
				HNSWOptions: &redis.FTHNSWOptions{
					Dim:            dimension,
					DistanceMetric: "L2",
					Type:           "FLOAT32",
				},
			},
		},
	).Result()
	require.NoError(t, err)

	testIDs, vectors := createRedisTestData()
	for i, vec := range vectors {
		buffer := floatsToBytes(vec)
		_, err = rdb.HSet(ctx,
			fmt.Sprintf("doc:%s", testIDs[i]),
			map[string]any{
				"content":   fmt.Sprintf("test content %d", i+1),
				"genre":     fmt.Sprintf("test genre %d", i+1),
				"embedding": buffer,
			},
		).Result()
		require.NoError(t, err)
	}

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:   qdrantHost,
		Port:   qdrantPort,
		APIKey: qdrantAPIKey,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	err = qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: testCollectionName,
		VectorsConfig: qdrant.NewVectorsConfigMap(
			map[string]*qdrant.VectorParams{
				"embedding": {
					Size:     uint64(dimension),
					Distance: qdrant.Distance_Euclid,
				},
			},
		),
	})
	require.NoError(t, err)

	args := []string{
		"redis",
		fmt.Sprintf("--redis.addr=%s", redisHost),
		fmt.Sprintf("--redis.index=%s", "vector_idx"),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, fmt.Sprint(qdrantPort)),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
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
		expectedPoints[fmt.Sprintf("doc:%s", id)] = struct {
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
		id := point.Payload[idField].GetStringValue()
		expected, exists := expectedPoints[id]
		require.True(t, exists)

		require.Equal(t, expected.content, point.Payload["content"].GetStringValue())
		require.Equal(t, expected.genre, point.Payload["genre"].GetStringValue())

		vector := point.Vectors.GetVectors().GetVectors()["embedding"].GetData()
		require.Equal(t, expected.vector, vector)
	}
}

func floatsToBytes(fs []float32) []byte {
	buf := make([]byte, len(fs)*4)
	for i, f := range fs {
		u := math.Float32bits(f)
		binary.LittleEndian.PutUint32(buf[i*4:], u)
	}
	return buf
}

func createRedisTestData() ([]string, [][]float32) {
	ids := make([]string, totalEntries)
	vectors := make([][]float32, totalEntries)

	for i := 0; i < totalEntries; i++ {
		ids[i] = fmt.Sprintf("%d", i+1)
		vectors[i] = randFloat32Values(dimension)
	}
	return ids, vectors
}
