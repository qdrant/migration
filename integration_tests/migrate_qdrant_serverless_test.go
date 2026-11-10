package integrationtests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/qdrant/go-client/qdrant"
	"github.com/qdrant/go-client/qdrant/serverless"
)

const (
	serverlessDenseVector  = "dense"
	serverlessSparseVector = "sparse"
	// serverlessVectorDelta is the difference that the default precision tier of Qdrant Serverless allows in a vector value.
	serverlessVectorDelta = 2e-3
)

// testMigrateQdrantServerless migrates a collection from a Qdrant cluster to Qdrant Serverless, and back to the cluster.
func testMigrateQdrantServerless(t *testing.T, numWorkers int) {
	serverlessURL := os.Getenv("QDRANT_SERVERLESS_URL")
	serverlessAPIKey := os.Getenv("QDRANT_SERVERLESS_API_KEY")
	if serverlessURL == "" || serverlessAPIKey == "" {
		t.Skip("Skipping Qdrant Serverless test. QDRANT_SERVERLESS_URL or QDRANT_SERVERLESS_API_KEY not set")
	}
	parsedURL, err := url.Parse(serverlessURL)
	require.NoError(t, err)

	ctx := context.Background()
	testID := fmt.Sprintf("serverless-test-%d", rand.Int())
	sourceCollection := testCollectionName + "-source"
	serverlessCollection := testCollectionName + "-" + testID
	targetCollection := testCollectionName + "-target"

	serverlessClient, err := serverless.NewClient(&qdrant.Config{Host: parsedURL.Hostname(), APIKey: serverlessAPIKey})
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, collection := range []string{serverlessCollection, serverlessCollection + "-offsets"} {
			_, err := serverlessClient.DeleteCollection(ctx, collection)
			require.NoError(t, err)
		}
		require.NoError(t, serverlessClient.Close())
	})

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
		VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
			serverlessDenseVector: {Size: dimension, Distance: qdrant.Distance_Cosine},
		}),
		SparseVectorsConfig: qdrant.NewSparseVectorsConfig(map[string]*qdrant.SparseVectorParams{
			serverlessSparseVector: {Modifier: qdrant.Modifier_Idf.Enum()},
		}),
	})
	require.NoError(t, err)
	for field, fieldType := range map[string]qdrant.FieldType{
		"name": qdrant.FieldType_FieldTypeKeyword,
		"age":  qdrant.FieldType_FieldTypeInteger,
		// A text index without parameters uses the word tokenizer.
		"city": qdrant.FieldType_FieldTypeText,
	} {
		_, err = qdrantClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
			CollectionName: sourceCollection,
			FieldName:      field,
			FieldType:      fieldType.Enum(),
			Wait:           qdrant.PtrOf(true),
		})
		require.NoError(t, err)
	}

	points := make([]*qdrant.PointStruct, totalEntries)
	for i := range points {
		// The largest point ID checks the stored offsets, which keep a numeric ID as a signed integer.
		id := uint64(i)
		if i == totalEntries-1 {
			id = math.MaxUint64
		}
		points[i] = &qdrant.PointStruct{
			Id: qdrant.NewIDNum(id),
			Vectors: qdrant.NewVectorsMap(map[string]*qdrant.Vector{
				serverlessDenseVector:  qdrant.NewVectorDense(randFloat32Values(dimension)),
				serverlessSparseVector: qdrant.NewVectorSparse([]uint32{uint32(i), uint32(i + 1000)}, []float32{rand.Float32(), rand.Float32()}),
			}),
			Payload: qdrant.NewValueMap(map[string]any{
				"name": fmt.Sprintf("name-%d", i),
				"age":  i,
				"city": fmt.Sprintf("City %d", i),
				"tags": []any{"a", i, nil, map[string]any{"nested": true}},
			}),
		}
	}
	_, err = qdrantClient.Upsert(ctx, &qdrant.UpsertPoints{CollectionName: sourceCollection, Points: points, Wait: qdrant.PtrOf(true)})
	require.NoError(t, err)

	runMigrationBinary(t, []string{
		"qdrant",
		fmt.Sprintf("--source.url=%s", qdrantURL),
		fmt.Sprintf("--source.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--source.collection=%s", sourceCollection),
		fmt.Sprintf("--target.url=%s", serverlessURL),
		fmt.Sprintf("--target.api-key=%s", serverlessAPIKey),
		fmt.Sprintf("--target.collection=%s", serverlessCollection),
		"--target.serverless",
		fmt.Sprintf("--migration.offsets-collection=%s-offsets", serverlessCollection),
		fmt.Sprintf("--migration.num-workers=%d", numWorkers),
		"--migration.batch-size=17",
	})

	config := serverlessCollectionConfig(ctx, t, serverlessClient, serverlessCollection)
	require.Equal(t, uint64(dimension), config.GetDenseVectors()[serverlessDenseVector].GetSize())
	require.Equal(t, serverless.Distance_COSINE, config.GetDenseVectors()[serverlessDenseVector].GetDistance())
	require.True(t, config.GetSparseVectors()[serverlessSparseVector].GetUseIdf())
	require.NotNil(t, config.GetPayloadIndexes()["name"].GetKeyword())
	require.NotNil(t, config.GetPayloadIndexes()["age"].GetInteger())
	require.Equal(t, serverless.Tokenizer_WORD, config.GetPayloadIndexes()["city"].GetText().GetTokenizer())

	runMigrationBinary(t, []string{
		"qdrant",
		fmt.Sprintf("--source.url=%s", serverlessURL),
		fmt.Sprintf("--source.api-key=%s", serverlessAPIKey),
		fmt.Sprintf("--source.collection=%s", serverlessCollection),
		"--source.serverless",
		fmt.Sprintf("--target.url=%s", qdrantURL),
		fmt.Sprintf("--target.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--target.collection=%s", targetCollection),
		fmt.Sprintf("--migration.offsets-collection=%s-offsets", targetCollection),
		fmt.Sprintf("--migration.num-workers=%d", numWorkers),
		"--migration.batch-size=13",
	})

	targetInfo, err := qdrantClient.GetCollectionInfo(ctx, targetCollection)
	require.NoError(t, err)
	require.Equal(t, qdrant.PayloadSchemaType_Keyword, targetInfo.GetPayloadSchema()["name"].GetDataType())
	require.Equal(t, qdrant.PayloadSchemaType_Integer, targetInfo.GetPayloadSchema()["age"].GetDataType())
	require.Equal(t, qdrant.TokenizerType_Word, targetInfo.GetPayloadSchema()["city"].GetParams().GetTextIndexParams().GetTokenizer())

	expected, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: sourceCollection,
		Limit:          qdrant.PtrOf(uint32(totalEntries)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	actual, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: targetCollection,
		Limit:          qdrant.PtrOf(uint32(totalEntries + 1)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, actual, totalEntries)

	for i, point := range actual {
		exp := expected[i]
		require.Equal(t, exp.GetId().GetNum(), point.GetId().GetNum())
		require.True(t, proto.Equal(&qdrant.Struct{Fields: exp.GetPayload()}, &qdrant.Struct{Fields: point.GetPayload()}), "payload of point %d", i)

		expVectors := exp.GetVectors().GetVectors().GetVectors()
		vectors := point.GetVectors().GetVectors().GetVectors()
		requireVectorsClose(t, expVectors[serverlessDenseVector].GetDenseVector().GetData(), vectors[serverlessDenseVector].GetDenseVector().GetData())
		expSparse, sparse := expVectors[serverlessSparseVector].GetSparseVector(), vectors[serverlessSparseVector].GetSparseVector()
		require.Equal(t, expSparse.GetIndices(), sparse.GetIndices())
		requireVectorsClose(t, expSparse.GetValues(), sparse.GetValues())
	}
}

func TestMigrateQdrantServerless(t *testing.T) {
	testMigrateQdrantServerless(t, 1)
}

func TestMigrateQdrantServerlessParallel(t *testing.T) {
	testMigrateQdrantServerless(t, 4)
}

// serverlessCollectionConfig returns the configuration of a serverless collection.
// Reads from Qdrant Serverless lag behind writes, so a new collection can show as missing for a few seconds.
func serverlessCollectionConfig(ctx context.Context, t *testing.T, client *serverless.Client, collection string) *serverless.CollectionConfig {
	for range 10 {
		resp, err := client.GetCollection(ctx, collection)
		require.NoError(t, err)
		if resp.GetExists() {
			return resp.GetConfig()
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("serverless collection %q does not exist", collection)
	return nil
}

func requireVectorsClose(t *testing.T, expected, actual []float32) {
	require.Len(t, actual, len(expected))
	for i := range expected {
		require.InDelta(t, expected[i], actual[i], serverlessVectorDelta, "value %d", i)
	}
}
