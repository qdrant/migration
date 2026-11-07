package integrationtests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/qdrant/go-client/qdrant"
)

// filterTenants are the tenants of the source collection. The last one has no points.
var filterTenants = []string{"acme", "globex", "initech", "nobody"}

// testMigrateFromQdrantWithFilter splits a collection into one target collection per tenant with --source.filter.
func testMigrateFromQdrantWithFilter(t *testing.T, numWorkers int) {
	ctx := context.Background()
	sourceCollection := testCollectionName + "-source"

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

	points := make([]*qdrant.PointStruct, totalEntries)
	for i := range points {
		points[i] = &qdrant.PointStruct{
			Id:      qdrant.NewIDNum(uint64(i)),
			Vectors: qdrant.NewVectorsDense(randFloat32Values(dimension)),
			Payload: qdrant.NewValueMap(map[string]any{"tenant": filterTenants[i%(len(filterTenants)-1)], "n": i}),
		}
	}
	_, err = qdrantClient.Upsert(ctx, &qdrant.UpsertPoints{CollectionName: sourceCollection, Points: points, Wait: qdrant.PtrOf(true)})
	require.NoError(t, err)

	for i, tenant := range filterTenants {
		filter := fmt.Sprintf(`{"must": [{"key": "tenant", "match": {"value": %q}}]}`, tenant)
		filterArg := "--source.filter=" + filter
		// The first tenant reads its filter from a file.
		if i == 0 {
			filterFile := filepath.Join(t.TempDir(), "filter.json")
			require.NoError(t, os.WriteFile(filterFile, []byte(filter), 0o600))
			filterArg = "--source.filter-file=" + filterFile
		}
		runMigrationBinary(t, []string{
			"qdrant",
			fmt.Sprintf("--source.url=%s", qdrantURL),
			fmt.Sprintf("--source.api-key=%s", qdrantAPIKey),
			fmt.Sprintf("--source.collection=%s", sourceCollection),
			filterArg,
			fmt.Sprintf("--target.url=%s", qdrantURL),
			fmt.Sprintf("--target.api-key=%s", qdrantAPIKey),
			fmt.Sprintf("--target.collection=%s", tenant),
			fmt.Sprintf("--migration.num-workers=%d", numWorkers),
			"--migration.batch-size=7",
		})
	}

	for _, tenant := range filterTenants {
		actual, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: tenant,
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
		require.Len(t, actual, len(expected), "points of tenant %q", tenant)
		for i, point := range actual {
			require.Equal(t, expected[i].GetId().GetNum(), point.GetId().GetNum())
			require.True(t, proto.Equal(&qdrant.Struct{Fields: expected[i].GetPayload()}, &qdrant.Struct{Fields: point.GetPayload()}))
		}
	}
}

func TestMigrateFromQdrantWithFilter(t *testing.T) {
	testMigrateFromQdrantWithFilter(t, 1)
}

func TestMigrateFromQdrantWithFilterParallel(t *testing.T) {
	testMigrateFromQdrantWithFilter(t, 4)
}
