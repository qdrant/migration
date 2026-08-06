package integrationtests

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

type targetShardRouting struct {
	flag     string
	fixedKey string
}

func testMigrateFromQdrantWithShardKeys(t *testing.T, sourceCollectionName, targetCollectionName string, numWorkers int, routing targetShardRouting) {
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
		Port:                   int(sourcePort.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer sourceClient.Close()

	targetClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   targetHost,
		Port:                   int(targetPort.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer targetClient.Close()

	createSource := &qdrant.CreateCollection{
		CollectionName: sourceCollectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     dimension,
			Distance: qdrant.Distance_Dot,
		}),
	}
	if routing.flag == "" {
		createSource.ShardingMethod = qdrant.ShardingMethod_Custom.Enum()
	}
	err = sourceClient.CreateCollection(ctx, createSource)
	require.NoError(t, err)

	shardKeys := []string{"shard_a", "shard_b"}

	if routing.flag == "" {
		for _, shardKey := range shardKeys {
			err = sourceClient.CreateShardKey(ctx, sourceCollectionName, &qdrant.CreateShardKey{
				ShardKey: qdrant.NewShardKey(shardKey),
			})
			require.NoError(t, err)
		}
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

		upsert := &qdrant.UpsertPoints{
			CollectionName: sourceCollectionName,
			Points:         points,
			Wait:           qdrant.PtrOf(true),
		}
		if routing.flag == "" {
			upsert.ShardKeySelector = &qdrant.ShardKeySelector{
				ShardKeys: []*qdrant.ShardKey{qdrant.NewShardKey(shardKey)},
			}
		}
		_, err = sourceClient.Upsert(ctx, upsert)
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
	if routing.flag != "" {
		args = append(args, routing.flag)
	}

	runMigrationBinary(t, args)

	targetCountResp, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: targetCollectionName,
		Exact:          qdrant.PtrOf(true),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(totalEntries), targetCountResp)

	targetShardKeys := shardKeys
	if routing.fixedKey != "" {
		targetShardKeys = []string{routing.fixedKey}
	}
	for _, shardKey := range targetShardKeys {
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
		expectedCount := totalEntries / 2
		if routing.fixedKey != "" {
			expectedCount = totalEntries
		}
		require.Len(t, points, expectedCount)

		for _, point := range points {
			require.Equal(t, shardKey, point.GetShardKey().GetKeyword())
			if routing.fixedKey == "" {
				require.Equal(t, shardKey, point.Payload["shard"].GetStringValue())
			}

			pointID := point.Id.GetUuid()
			expectedVector := expectedPointsByID[pointID]
			actualVector := point.Vectors.GetVector().GetDenseVector().GetData()
			require.Equal(t, expectedVector, actualVector)
		}
	}
}

func TestMigrateFromQdrantWithShardKeys(t *testing.T) {
	testMigrateFromQdrantWithShardKeys(t, "source_collection", "target_collection", 1, targetShardRouting{})
}

func TestMigrateFromQdrantWithShardKeysParallel(t *testing.T) {
	testMigrateFromQdrantWithShardKeys(t, "source_collection_parallel", "target_collection_parallel", 4, targetShardRouting{})
}

func TestMigrateFromQdrantWithShardKeyField(t *testing.T) {
	testMigrateFromQdrantWithShardKeys(t, "source_collection_flat", "target_collection_custom", 4, targetShardRouting{
		flag: "--target.shard-key-field=shard",
	})
}

func TestMigrateFromQdrantWithTargetShardKey(t *testing.T) {
	testMigrateFromQdrantWithShardKeys(t, "source_collection_fixed", "target_collection_fixed", 4, targetShardRouting{
		flag:     "--target.shard-key=target_shard",
		fixedKey: "target_shard",
	})
}
