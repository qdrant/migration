package integrationtests

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

func testMigrateFromQdrantWithShardKeys(t *testing.T, sourceCollectionName, targetCollectionName string, numWorkers int) {
	ctx := context.Background()

	sourceContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		if err := sourceContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate source Qdrant container: %v", err)
		}
	}()

	targetContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		if err := targetContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate target Qdrant container: %v", err)
		}
	}()

	sourceHost, err := sourceContainer.Host(ctx)
	require.NoError(t, err)
	sourcePort, err := sourceContainer.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)

	targetHost, err := targetContainer.Host(ctx)
	require.NoError(t, err)
	targetPort, err := targetContainer.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)

	sourceClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   sourceHost,
		Port:                   sourcePort.Int(),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer sourceClient.Close()

	targetClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   targetHost,
		Port:                   targetPort.Int(),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer targetClient.Close()

	err = sourceClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: sourceCollectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     dimension,
			Distance: qdrant.Distance_Dot,
		}),
		ShardingMethod: qdrant.ShardingMethod_Custom.Enum(),
	})
	require.NoError(t, err)

	shardKeys := []string{"shard_a", "shard_b"}

	for _, shardKey := range shardKeys {
		err = sourceClient.CreateShardKey(ctx, sourceCollectionName, &qdrant.CreateShardKey{
			ShardKey: qdrant.NewShardKey(shardKey),
		})
		require.NoError(t, err)
	}

	expectedPointsByID := make(map[string][]float32)

	for _, shardKey := range shardKeys {
		points := make([]*qdrant.PointStruct, 0)
		for i := 0; i < totalEntries/2; i++ {
			pointID := uuid.New().String()
			vector := randFloat32Values(dimension)
			expectedPointsByID[pointID] = vector

			points = append(points, &qdrant.PointStruct{
				Id:      qdrant.NewID(pointID),
				Vectors: qdrant.NewVectors(vector...),
				Payload: qdrant.NewValueMap(map[string]any{
					"shard": shardKey,
				}),
			})
		}

		_, err = sourceClient.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: sourceCollectionName,
			Points:         points,
			ShardKeySelector: &qdrant.ShardKeySelector{
				ShardKeys: []*qdrant.ShardKey{qdrant.NewShardKey(shardKey)},
			},
			Wait: qdrant.PtrOf(true),
		})
		require.NoError(t, err)
	}

	args := []string{
		"qdrant",
		fmt.Sprintf("--source.url=http://%s:%s", sourceHost, sourcePort.Port()),
		fmt.Sprintf("--source.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--source.collection=%s", sourceCollectionName),
		fmt.Sprintf("--target.url=http://%s:%s", targetHost, targetPort.Port()),
		fmt.Sprintf("--target.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--target.collection=%s", targetCollectionName),
		fmt.Sprintf("--migration.num-workers=%d", numWorkers),
		"--migration.create-collection=true",
	}

	runMigrationBinary(t, args)

	targetCountResp, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: targetCollectionName,
		Exact:          qdrant.PtrOf(true),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(totalEntries), targetCountResp)

	for _, shardKey := range shardKeys {
		points, err := targetClient.Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: targetCollectionName,
			Limit:          qdrant.PtrOf(uint32(totalEntries)),
			WithPayload:    qdrant.NewWithPayload(true),
			WithVectors:    qdrant.NewWithVectors(true),
			ShardKeySelector: &qdrant.ShardKeySelector{
				ShardKeys: []*qdrant.ShardKey{qdrant.NewShardKey(shardKey)},
			},
		})
		require.NoError(t, err)
		require.Len(t, points, totalEntries/2)

		for _, point := range points {
			require.Equal(t, shardKey, point.GetShardKey().GetKeyword())
			require.Equal(t, shardKey, point.Payload["shard"].GetStringValue())

			pointID := point.Id.GetUuid()
			expectedVector := expectedPointsByID[pointID]
			actualVector := point.Vectors.GetVector().GetDenseVector().GetData()
			require.Equal(t, expectedVector, actualVector)
		}
	}
}

func TestMigrateFromQdrantWithShardKeys(t *testing.T) {
	testMigrateFromQdrantWithShardKeys(t, "source_collection", "target_collection", 1)
}

func TestMigrateFromQdrantWithShardKeysParallel(t *testing.T) {
	testMigrateFromQdrantWithShardKeys(t, "source_collection_parallel", "target_collection_parallel", 4)
}
