package integrationtests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/qdrant/go-client/qdrant"
)

// testMigrateFromQdrantWithSplit splits a collection into one target collection per tenant in a single migration.
// The filter leaves out the last tenant. With extraArgs, the migration also gets those flags.
func testMigrateFromQdrantWithSplit(t *testing.T, numWorkers int, extraArgs ...string) {
	ctx := context.Background()
	sourceCollection := testCollectionName + "-source"
	tenants := []string{"acme", "globex", "initech"}

	qdrantContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		if err := qdrantContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Qdrant container: %v", err)
		}
	}()
	qdrantHost, err := qdrantContainer.Host(ctx)
	require.NoError(t, err)
	qdrantPort, err := qdrantContainer.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantURL := fmt.Sprintf("http://%s:%s", qdrantHost, qdrantPort.Port())

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   int(qdrantPort.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	err = qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: sourceCollection,
		VectorsConfig:  qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: dimension, Distance: qdrant.Distance_Dot}),
	})
	require.NoError(t, err)
	_, err = qdrantClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName: sourceCollection,
		FieldName:      "tenant",
		FieldType:      qdrant.FieldType_FieldTypeKeyword.Enum(),
		Wait:           qdrant.PtrOf(true),
	})
	require.NoError(t, err)

	points := make([]*qdrant.PointStruct, totalEntries)
	for i := range points {
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewIDNum(uint64(i)),
			Vectors: qdrant.NewVectorsDense(randFloat32Values(dimension)),
			Payload: qdrant.NewValueMap(map[string]any{"tenant": tenants[i%len(tenants)], "tier": []string{"gold", "silver"}[i%2], "n": i}),
		}
	}
	_, err = qdrantClient.Upsert(ctx, &qdrant.UpsertPoints{CollectionName: sourceCollection, Points: points, Wait: qdrant.PtrOf(true)})
	require.NoError(t, err)

	runMigrationBinary(t, append([]string{
		"qdrant",
		fmt.Sprintf("--source.url=%s", qdrantURL),
		fmt.Sprintf("--source.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--source.collection=%s", sourceCollection),
		`--source.filter={"must_not": [{"key": "tenant", "match": {"value": "initech"}}]}`,
		fmt.Sprintf("--target.url=%s", qdrantURL),
		fmt.Sprintf("--target.api-key=%s", qdrantAPIKey),
		"--target.collection=org-{tenant}",
		fmt.Sprintf("--migration.num-workers=%d", numWorkers),
		"--migration.batch-size=7",
	}, extraArgs...))

	exists, err := qdrantClient.CollectionExists(ctx, "org-initech")
	require.NoError(t, err)
	require.False(t, exists, "the filter leaves out the tenant initech")

	for _, tenant := range tenants[:2] {
		collection := "org-" + tenant
		info, err := qdrantClient.GetCollectionInfo(ctx, collection)
		require.NoError(t, err)
		require.Contains(t, info.GetPayloadSchema(), "tenant", "payload index of %s", collection)

		actual, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: collection,
			Limit:          qdrant.PtrOf(uint32(totalEntries)),
			WithPayload:    qdrant.NewWithPayload(true),
		})
		require.NoError(t, err)

		var expected []*qdrant.PointStruct
		for _, point := range points {
			if point.GetPayload()["tenant"].GetStringValue() == tenant {
				expected = append(expected, point)
			}
		}
		require.Len(t, actual, len(expected), "points of %s", collection)
		for i, point := range actual {
			require.Equal(t, expected[i].GetId().GetNum(), point.GetId().GetNum())
			require.True(t, proto.Equal(&qdrant.Struct{Fields: expected[i].GetPayload()}, &qdrant.Struct{Fields: point.GetPayload()}))
		}
	}
}

func TestMigrateFromQdrantWithSplit(t *testing.T) {
	testMigrateFromQdrantWithSplit(t, 1)
}

func TestMigrateFromQdrantWithSplitParallel(t *testing.T) {
	testMigrateFromQdrantWithSplit(t, 4)
}

// Each target collection of the split gets its own shard keys.
func TestMigrateFromQdrantWithSplitAndShardKeyField(t *testing.T) {
	testMigrateFromQdrantWithSplit(t, 4, "--target.shard-key-field=tier")
}
