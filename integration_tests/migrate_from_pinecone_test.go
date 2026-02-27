package integrationtests

import (
	"context"
	"fmt"
	"testing"

	"github.com/pinecone-io/go-pinecone/v3/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/qdrant/go-client/qdrant"
)

const (
	sparseVectorName = "sparse_vector"
)

func TestMigrateFromPineconeDense(t *testing.T) {
	ctx := context.Background()

	pineconeHost, pineconeIndexHost, qdrantHost, qdrantPort := setupContainers(t, ctx)

	pineconeClient, err := pinecone.NewClient(pinecone.NewClientParams{
		Host:   pineconeHost,
		ApiKey: "qdrant-migration-test",
	})
	require.NoError(t, err)

	metric := pinecone.Euclidean
	dims := int32(dimension)

	indexName := "my-serverless-index"
	vectorType := "dense"

	_, err = pineconeClient.CreateServerlessIndex(ctx, &pinecone.CreateServerlessIndexRequest{
		Name:       indexName,
		Dimension:  &dims,
		Metric:     &metric,
		Cloud:      pinecone.Aws,
		Region:     "us-east-1",
		VectorType: &vectorType,
	})
	require.NoError(t, err)

	indexConn, err := pineconeClient.Index(pinecone.NewIndexConnParams{
		Host: pineconeIndexHost,
	})
	require.NoError(t, err)

	testIDs, vectors := createTestData(t, false)
	_, err = indexConn.UpsertVectors(ctx, vectors)
	require.NoError(t, err)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   qdrantPort,
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	args := []string{
		"pinecone",
		fmt.Sprintf("--pinecone.index-name=%s", indexName),
		fmt.Sprintf("--pinecone.index-host=%s", pineconeIndexHost),
		fmt.Sprintf("--pinecone.service-host=%s", pineconeHost),
		fmt.Sprintf("--pinecone.api-key=%s", "qdrant-migration-test"),
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
		source string
		vector []float32
	})
	for i, id := range testIDs {
		expectedPoints[id] = struct {
			source string
			vector []float32
		}{
			source: vectors[i].Metadata.Fields["source"].GetStringValue(),
			vector: *vectors[i].Values,
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		expected, exists := expectedPoints[id]
		require.True(t, exists)

		require.Equal(t, expected.source, point.Payload["source"].GetStringValue())

		vector := point.Vectors.GetVector().GetDenseVector().GetData()
		require.Equal(t, expected.vector, vector)
	}
}

func TestMigrateFromPineconeSparse(t *testing.T) {
	ctx := context.Background()

	pineconeHost, pineconeIndexHost, qdrantHost, qdrantPort := setupContainers(t, ctx)

	pineconeClient, err := pinecone.NewClient(pinecone.NewClientParams{
		Host:   pineconeHost,
		ApiKey: "qdrant-migration-test",
	})
	require.NoError(t, err)

	indexName := "my-serverless-index"
	vectorType := "sparse"

	_, err = pineconeClient.CreateServerlessIndex(ctx, &pinecone.CreateServerlessIndexRequest{
		Name:       indexName,
		Cloud:      pinecone.Aws,
		Region:     "us-east-1",
		VectorType: &vectorType,
	})
	require.NoError(t, err)

	indexConn, err := pineconeClient.Index(pinecone.NewIndexConnParams{
		Host: pineconeIndexHost,
	})
	require.NoError(t, err)

	testIDs, vectors := createTestData(t, true)
	_, err = indexConn.UpsertVectors(ctx, vectors)
	require.NoError(t, err)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   qdrantPort,
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	args := []string{
		"pinecone",
		fmt.Sprintf("--pinecone.index-name=%s", indexName),
		fmt.Sprintf("--pinecone.index-host=%s", pineconeIndexHost),
		fmt.Sprintf("--pinecone.service-host=%s", pineconeHost),
		fmt.Sprintf("--pinecone.api-key=%s", "qdrant-migration-test"),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, fmt.Sprint(qdrantPort)),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		fmt.Sprintf("--qdrant.sparse-vector=%s", sparseVectorName),
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
		source string
		vector *pinecone.SparseValues
	})
	for i, id := range testIDs {
		expectedPoints[id] = struct {
			source string
			vector *pinecone.SparseValues
		}{
			source: vectors[i].Metadata.Fields["source"].GetStringValue(),
			vector: vectors[i].SparseValues,
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		expected, exists := expectedPoints[id]
		require.True(t, exists, "missing expected point with id: %s", id)

		require.Equal(t, expected.source, point.Payload["source"].GetStringValue(), "source mismatch for id: %s", id)

		actualSparseVec := point.Vectors.GetVectors().GetVectors()[sparseVectorName].GetSparseVector()
		actualValues := actualSparseVec.Values
		actualIndices := actualSparseVec.Indices

		expectedMap := make(map[uint32]float32, len(expected.vector.Indices))
		for i, idx := range expected.vector.Indices {
			expectedMap[idx] = expected.vector.Values[i]
		}

		actualMap := make(map[uint32]float32, len(actualIndices))
		for i, idx := range actualIndices {
			actualMap[idx] = actualValues[i]
		}

		require.Len(t, expectedMap, len(actualMap), "sparse vector index count mismatch for id: %s", id)

		for idx, expectedVal := range expectedMap {
			actualVal, exists := actualMap[idx]
			require.True(t, exists, "missing index %d in sparse vector for id: %s", idx, id)
			require.InEpsilon(t, expectedVal, actualVal, 1e-2, "mismatched value at index %d for id: %s", idx, id)
		}
	}

}

func setupContainers(t *testing.T, ctx context.Context) (pHost, pIndexHost, qHost string, qPort int) {
	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	pineconeCont := pineconeContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, pineconeCont.Terminate(ctx))
	})

	var err error
	pHost, err = pineconeCont.PortEndpoint(ctx, "5081/tcp", "http")
	require.NoError(t, err)
	pIndexHost, err = pineconeCont.PortEndpoint(ctx, "5082/tcp", "http")
	require.NoError(t, err)

	qHost, err = qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qPort = mappedPort.Int()

	return
}

func createTestData(t *testing.T, isSparse bool) ([]string, []*pinecone.Vector) {
	ids := make([]string, totalEntries)
	vectors := make([]*pinecone.Vector, totalEntries)

	for i := 0; i < totalEntries; i++ {
		ids[i] = fmt.Sprintf("%d", i+1)
		metadata, err := structpb.NewStruct(map[string]interface{}{
			"source": fmt.Sprintf("test%d", i+1),
		})
		require.NoError(t, err)

		vec := &pinecone.Vector{
			Id:       ids[i],
			Metadata: metadata,
		}

		if isSparse {
			sparse := &pinecone.SparseValues{
				Indices: randIndices(5),
				Values:  randFloat32Values(5),
			}
			vec.SparseValues = sparse
		} else {
			dense := randFloat32Values(dimension)
			vec.Values = &dense
		}

		vectors[i] = vec
	}
	return ids, vectors
}
