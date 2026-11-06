package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/pinecone-io/go-pinecone/v3/pinecone"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/qdrant/go-client/qdrant"
)

const (
	pineconeControlPlane = "https://api.pinecone.io"
	pineconeAPIVersion   = "2026-07"
	sparseVectorName     = "sparse_vector"
)

// Each test creates its own Pinecone index and deletes it after the test.

// TestMigrateFromPineconeVectors reads a dense and a sparse index through the vectors API.
func TestMigrateFromPineconeVectors(t *testing.T) {
	ctx := context.Background()
	apiKey := pineconeAPIKey(t)
	client, err := pinecone.NewClient(pinecone.NewClientParams{ApiKey: apiKey})
	require.NoError(t, err)

	for _, tc := range []struct {
		vectorType string
		metric     pinecone.IndexMetric
		namespace  string
	}{
		// A namespace other than the default one checks --pinecone.namespace.
		{"dense", pinecone.Euclidean, "migration-test"},
		{"sparse", pinecone.Dotproduct, ""},
	} {
		t.Run(tc.vectorType, func(t *testing.T) {
			index := createVectorIndex(ctx, t, client, tc.vectorType, tc.metric)
			indexConn, err := client.Index(pinecone.NewIndexConnParams{Host: index.Host, Namespace: tc.namespace})
			require.NoError(t, err)
			ids, vectors := createTestData(t, tc.vectorType == "sparse")
			_, err = indexConn.UpsertVectors(ctx, vectors)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				stats, err := indexConn.DescribeIndexStats(ctx)
				return err == nil && stats.Namespaces[tc.namespace] != nil && stats.Namespaces[tc.namespace].VectorCount == totalEntries
			}, 2*time.Minute, 2*time.Second, "the vectors of index %q are not readable", index.Name)

			qdrantClient, qdrantHost, qdrantPort := startQdrant(ctx, t)
			runMigrationBinary(t, pineconeArgs(apiKey, index.Name, index.Host, qdrantHost, qdrantPort, "--pinecone.namespace="+tc.namespace))

			points := scrollPoints(ctx, t, qdrantClient)
			for i, id := range ids {
				point, ok := points[id]
				require.True(t, ok, "point for vector %q not found", id)
				require.Equal(t, vectors[i].Metadata.Fields["source"].GetStringValue(), point.Payload["source"].GetStringValue())
				if sparse := vectors[i].SparseValues; sparse != nil {
					requireSparseEqual(t, sparse.Indices, sparse.Values, point.Vectors.GetVectors().GetVectors()[sparseVectorName].GetSparseVector())
				} else {
					require.InDeltaSlice(t, *vectors[i].Values, point.Vectors.GetVector().GetDenseVector().GetData(), 1e-6)
				}
			}
		})
	}
}

// TestMigrateFromPineconeDocuments creates an index with a document schema, which the migration reads
// through the documents API.
func TestMigrateFromPineconeDocuments(t *testing.T) {
	ctx := context.Background()
	apiKey := pineconeAPIKey(t)

	indexName := fmt.Sprintf("migration-test-%d", rand.Int())
	require.NoError(t, pineconeREST(ctx, apiKey, http.MethodPost, pineconeControlPlane+"/indexes", map[string]any{
		"name": indexName,
		"schema": map[string]any{"fields": map[string]any{
			"title":     map[string]any{"type": "string", "full_text_search": map[string]any{}},
			"body":      map[string]any{"type": "string", "full_text_search": map[string]any{}},
			"embedding": map[string]any{"type": "dense_vector", "dimension": dimension, "metric": "cosine"},
			"sparse":    map[string]any{"type": "sparse_vector"},
		}},
	}, nil))
	t.Cleanup(func() {
		require.NoError(t, pineconeREST(context.WithoutCancel(ctx), apiKey, http.MethodDelete, pineconeControlPlane+"/indexes/"+indexName, nil, nil))
	})

	var index struct {
		Host   string `json:"host"`
		Status struct {
			Ready bool `json:"ready"`
		} `json:"status"`
	}
	require.Eventually(t, func() bool {
		err := pineconeREST(ctx, apiKey, http.MethodGet, pineconeControlPlane+"/indexes/"+indexName, nil, &index)
		return err == nil && index.Status.Ready
	}, 5*time.Minute, 2*time.Second, "index %q is not ready", indexName)

	// Only documents with an even number have a sparse vector.
	documents := make([]map[string]any, totalEntries)
	for i := range documents {
		documents[i] = map[string]any{
			"_id":       fmt.Sprintf("doc-%03d", i),
			"title":     fmt.Sprintf("title %d", i),
			"body":      fmt.Sprintf("body of document %d", i),
			"embedding": randFloat32Values(dimension),
			"year":      2000 + i,
			"tags":      []string{fmt.Sprintf("tag-%d", i%3)},
		}
		if i%2 == 0 {
			documents[i]["sparse"] = map[string]any{
				"indices": []uint32{uint32(i), uint32(i) + 100, uint32(i) + 200},
				"values":  randFloat32Values(3),
			}
		}
	}
	documentsURL := "https://" + index.Host + "/namespaces/__default__/documents/"
	require.NoError(t, pineconeREST(ctx, apiKey, http.MethodPost, documentsURL+"upsert", map[string]any{"documents": documents}, nil))

	require.Eventually(t, func() bool {
		var stats struct {
			TotalVectorCount int `json:"totalVectorCount"`
		}
		err := pineconeREST(ctx, apiKey, http.MethodPost, "https://"+index.Host+"/describe_index_stats", map[string]any{}, &stats)
		return err == nil && stats.TotalVectorCount == totalEntries
	}, 2*time.Minute, 2*time.Second, "the documents of index %q are not readable", indexName)

	qdrantClient, qdrantHost, qdrantPort := startQdrant(ctx, t)
	runMigrationBinary(t, pineconeArgs(apiKey, indexName, index.Host, qdrantHost, qdrantPort))

	info, err := qdrantClient.GetCollectionInfo(ctx, testCollectionName)
	require.NoError(t, err)
	dense := info.GetConfig().GetParams().GetVectorsConfig().GetParamsMap().GetMap()
	require.Len(t, dense, 1)
	require.Equal(t, uint64(dimension), dense["embedding"].GetSize())
	require.Equal(t, qdrant.Distance_Cosine, dense["embedding"].GetDistance())
	require.Contains(t, info.GetConfig().GetParams().GetSparseVectorsConfig().GetMap(), "sparse")
	for _, field := range []string{"title", "body"} {
		require.Equal(t, qdrant.PayloadSchemaType_Text, info.GetPayloadSchema()[field].GetDataType(), "text index on %q", field)
	}

	points := scrollPoints(ctx, t, qdrantClient)
	for i, document := range documents {
		point, ok := points[document["_id"].(string)]
		require.True(t, ok, "point for document %q not found", document["_id"])

		require.Equal(t, document["title"], point.Payload["title"].GetStringValue())
		require.Equal(t, document["body"], point.Payload["body"].GetStringValue())
		require.InDelta(t, float64(2000+i), point.Payload["year"].GetDoubleValue(), 0)
		require.Equal(t, document["tags"].([]string)[0], point.Payload["tags"].GetListValue().GetValues()[0].GetStringValue())
		require.NotContains(t, point.Payload, "embedding")
		require.NotContains(t, point.Payload, "sparse")

		vectors := point.Vectors.GetVectors().GetVectors()
		expected := normalizedVector(document["embedding"].([]float32))
		require.InDeltaSlice(t, expected, vectors["embedding"].GetDenseVector().GetData(), 1e-5)

		sparse, ok := document["sparse"].(map[string]any)
		if !ok {
			require.NotContains(t, vectors, "sparse", "document %q has no sparse vector", document["_id"])
			continue
		}
		requireSparseEqual(t, sparse["indices"].([]uint32), sparse["values"].([]float32), vectors["sparse"].GetSparseVector())
	}
}

func pineconeAPIKey(t *testing.T) string {
	t.Helper()
	apiKey := os.Getenv("PINECONE_API_KEY")
	if apiKey == "" {
		t.Skip("Skipping Pinecone test. PINECONE_API_KEY not set")
	}
	return apiKey
}

// createVectorIndex creates a serverless vector index and deletes it after the test.
func createVectorIndex(ctx context.Context, t *testing.T, client *pinecone.Client, vectorType string, metric pinecone.IndexMetric) *pinecone.Index {
	t.Helper()
	request := &pinecone.CreateServerlessIndexRequest{
		Name:       fmt.Sprintf("migration-test-%d", rand.Int()),
		Cloud:      pinecone.Aws,
		Region:     "us-east-1",
		Metric:     &metric,
		VectorType: &vectorType,
	}
	if vectorType == "dense" {
		request.Dimension = qdrant.PtrOf(int32(dimension))
	}
	_, err := client.CreateServerlessIndex(ctx, request)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.DeleteIndex(context.WithoutCancel(ctx), request.Name))
	})

	var index *pinecone.Index
	require.Eventually(t, func() bool {
		index, err = client.DescribeIndex(ctx, request.Name)
		return err == nil && index.Status.Ready
	}, 5*time.Minute, 2*time.Second, "index %q is not ready", request.Name)
	return index
}

func pineconeArgs(apiKey, indexName, indexHost, qdrantHost string, qdrantPort int, extra ...string) []string {
	return append([]string{
		"pinecone",
		fmt.Sprintf("--pinecone.index-name=%s", indexName),
		// The host has no scheme, as Pinecone returns it.
		fmt.Sprintf("--pinecone.index-host=%s", indexHost),
		fmt.Sprintf("--pinecone.api-key=%s", apiKey),
		fmt.Sprintf("--qdrant.url=http://%s:%d", qdrantHost, qdrantPort),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		"--migration.batch-size=7",
	}, extra...)
}

// scrollPoints returns all points of the test collection by their Pinecone ID.
func scrollPoints(ctx context.Context, t *testing.T, client *qdrant.Client) map[string]*qdrant.RetrievedPoint {
	t.Helper()
	points, err := client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(totalEntries + 1)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, totalEntries)

	byID := make(map[string]*qdrant.RetrievedPoint, len(points))
	for _, point := range points {
		byID[point.Payload[idField].GetStringValue()] = point
	}
	return byID
}

// requireSparseEqual compares sparse vectors by index, because the order of the indices can change.
func requireSparseEqual(t *testing.T, indices []uint32, values []float32, actual *qdrant.SparseVector) {
	t.Helper()
	require.ElementsMatch(t, indices, actual.GetIndices())
	actualValues := make(map[uint32]float32, len(actual.GetIndices()))
	for i, index := range actual.GetIndices() {
		actualValues[index] = actual.GetValues()[i]
	}
	for i, index := range indices {
		require.InDelta(t, values[i], actualValues[index], 1e-6)
	}
}

func startQdrant(ctx context.Context, t *testing.T) (*qdrant.Client, string, int) {
	t.Helper()
	container := qdrantContainer(ctx, t, qdrantAPIKey)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   host,
		Port:                   int(port.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client, host, int(port.Num())
}

// pineconeREST sends a request to the Pinecone REST API, which the Go client of this module does not
// support for indexes with a document schema.
func pineconeREST(ctx context.Context, apiKey, method, url string, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Api-Key", apiKey)
	req.Header.Set("X-Pinecone-API-Version", pineconeAPIVersion)
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("%s %s returned status %d: %s", method, url, res.StatusCode, data)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func createTestData(t *testing.T, isSparse bool) ([]string, []*pinecone.Vector) {
	ids := make([]string, totalEntries)
	vectors := make([]*pinecone.Vector, totalEntries)
	for i := range vectors {
		ids[i] = fmt.Sprintf("%d", i+1)
		metadata, err := structpb.NewStruct(map[string]any{"source": fmt.Sprintf("test%d", i+1)})
		require.NoError(t, err)
		vectors[i] = &pinecone.Vector{Id: ids[i], Metadata: metadata}
		if isSparse {
			vectors[i].SparseValues = &pinecone.SparseValues{Indices: randIndices(5), Values: randFloat32Values(5)}
		} else {
			dense := randFloat32Values(dimension)
			vectors[i].Values = &dense
		}
	}
	return ids, vectors
}
