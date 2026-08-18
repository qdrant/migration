package integrationtests

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
	"github.com/qdrant/migration/pkg/commons"
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
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort := mappedPort.Num()

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
		Host:                   qdrantHost,
		Port:                   int(qdrantPort),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
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

		vector := point.Vectors.GetVectors().GetVectors()["embedding"].GetDenseVector().GetData()
		require.Equal(t, expected.vector, vector)
	}
}

func TestMigrateFromRedisVectorSets(t *testing.T) {
	ctx := context.Background()
	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	redisCont := redisVectorSetContainer(ctx, t)
	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, redisCont.Terminate(ctx))
	})

	redisHost, err := redisCont.PortEndpoint(ctx, "6379/tcp", "")
	require.NoError(t, err)
	qdrantHost, err := qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort := mappedPort.Num()

	rdb := redis.NewClient(&redis.Options{Addr: redisHost})
	defer rdb.Close()

	const vectorDim = 2
	type vectorSetElement struct {
		key, member, content, tenantID string
		vector                         []float64
	}
	elements := []vectorSetElement{
		{"tenant:alice:vector_set", "alice:1", "alice content 1", "alice", []float64{1, 2}},
		{"tenant:alice:vector_set", "alice:2", "alice content 2", "alice", []float64{3, 4}},
		{"tenant:bob:vector_set", "bob:1", "bob content 1", "bob", []float64{5, 6}},
	}

	for _, element := range elements {
		_, err = rdb.VAdd(ctx, element.key, element.member, &redis.VectorValues{Val: element.vector}).Result()
		require.NoError(t, err)
		attrs, err := json.Marshal(map[string]string{"content": element.content})
		require.NoError(t, err)
		_, err = rdb.VSetAttr(ctx, element.key, element.member, string(attrs)).Result()
		require.NoError(t, err)
	}

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   int(qdrantPort),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	err = qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: testCollectionName,
		VectorsConfig:  qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: vectorDim, Distance: qdrant.Distance_Euclid}),
	})
	require.NoError(t, err)

	// A sentinel vector proves that resume skips the checkpointed member.
	resumed := elements[0]
	sentinel := []float32{0.25, 0.25}
	resumedPointID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(resumed.key+":"+resumed.member)).String()
	_, err = qdrantClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: testCollectionName,
		Wait:           qdrant.PtrOf(true),
		Points: []*qdrant.PointStruct{{
			Id: qdrant.NewIDUUID(resumedPointID), Vectors: qdrant.NewVectors(sentinel...),
			Payload: qdrant.NewValueMap(map[string]any{"content": resumed.content, idField: resumed.member, "tenant_id": resumed.tenantID}),
		}},
	})
	require.NoError(t, err)

	const offsetsCollection = "_migration_offsets"
	require.NoError(t, commons.PrepareOffsetsCollection(ctx, offsetsCollection, qdrantClient))
	cursorJSON, err := json.Marshal(map[string]string{"key": resumed.key, "member": resumed.member})
	require.NoError(t, err)
	require.NoError(t, commons.StoreStartOffset(ctx, offsetsCollection, qdrantClient, "vectorset|tenant:*:vector_set", qdrant.NewIDUUID(string(cursorJSON)), 1))

	args := []string{
		"redis",
		fmt.Sprintf("--redis.addr=%s", redisHost),
		"--redis.source=vectorset",
		"--redis.key-pattern=tenant:*:vector_set",
		"--redis.tenant-regex=tenant:(?P<tenant_id>[^:]+):vector_set",
		"--migration.batch-size=2",
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, fmt.Sprint(qdrantPort)),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
	}

	runMigrationBinary(t, args)

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(10)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(elements))

	expected := make(map[string]vectorSetElement, len(elements))
	for _, element := range elements {
		expected[element.member] = element
	}
	resumed.vector = []float64{0.25, 0.25}
	expected[resumed.member] = resumed

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		want, exists := expected[id]
		require.True(t, exists, "unexpected point ID: %s", id)
		require.Equal(t, want.content, point.Payload["content"].GetStringValue())
		require.Equal(t, want.tenantID, point.Payload["tenant_id"].GetStringValue(), "unexpected tenant for point %s", id)
		expectedPointID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(want.key+":"+want.member)).String()
		require.Equal(t, expectedPointID, point.Id.GetUuid())

		vector := point.Vectors.GetVector().GetDenseVector().GetData()
		require.Len(t, vector, vectorDim)
		for i, f := range vector {
			require.InDelta(t, want.vector[i], float64(f), 0.1)
		}
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
