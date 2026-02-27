package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

func TestMigrateFromElasticsearch(t *testing.T) {
	ctx := context.Background()

	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	elasticsearchCont := elasticsearchContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, elasticsearchCont.Terminate(ctx))
	})

	esHost, err := elasticsearchCont.PortEndpoint(ctx, "9200/tcp", "http")
	require.NoError(t, err)

	qdrantHost, err := qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort := mappedPort.Int()

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esHost},
	})
	require.NoError(t, err)

	indexName := "test-index"
	settings := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"vector": map[string]interface{}{
					"type":       "dense_vector",
					"dims":       dimension,
					"index":      true,
					"similarity": "l2_norm",
				},
				"doc":    map[string]interface{}{"type": "text"},
				"source": map[string]interface{}{"type": "text"},
			},
		},
	}
	mappingBody, err := json.Marshal(settings)
	require.NoError(t, err)

	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  bytes.NewReader(mappingBody),
	}
	res, err := req.Do(ctx, esClient)
	require.NoError(t, err)
	defer res.Body.Close()
	require.False(t, res.IsError(), "Failed to create index")

	testIDs := make([]string, totalEntries)
	testVectors := make([][]float32, totalEntries)
	testDocs := make([]string, totalEntries)
	testSources := make([]string, totalEntries)

	for i := 0; i < totalEntries; i++ {
		testIDs[i] = fmt.Sprintf("%d", i+1)
		testVectors[i] = randFloat32Values(dimension)
		testDocs[i] = fmt.Sprintf("test doc %d", i+1)
		testSources[i] = fmt.Sprintf("source%d", i+1)

		doc := map[string]interface{}{
			"vector": testVectors[i],
			"doc":    testDocs[i],
			"source": testSources[i],
		}
		body, err := json.Marshal(doc)
		require.NoError(t, err)

		indexReq := esapi.IndexRequest{
			Index:      indexName,
			DocumentID: testIDs[i],
			Body:       bytes.NewReader(body),
			Refresh:    "true",
		}
		res, err := indexReq.Do(ctx, esClient)
		require.NoError(t, err)
		defer res.Body.Close()

		if res.IsError() {
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				t.Fatalf("Failed to parse error response: %v", err)
			}
			t.Fatalf("Failed to index document: %v", e)
		}
	}

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   qdrantPort,
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	args := []string{
		"elasticsearch",
		fmt.Sprintf("--elasticsearch.url=%s", esHost),
		fmt.Sprintf("--elasticsearch.index=%s", indexName),
		fmt.Sprintf("--qdrant.url=http://%s:%d", qdrantHost, qdrantPort),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		"--elasticsearch.insecure-skip-verify",
	}

	runMigrationBinary(t, args)

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(totalEntries)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, totalEntries)

	expectedPoints := make(map[string]struct {
		doc    string
		source string
		vector []float32
	})
	for i, id := range testIDs {
		expectedPoints[id] = struct {
			doc    string
			source string
			vector []float32
		}{
			doc:    testDocs[i],
			source: testSources[i],
			vector: testVectors[i],
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		expected, exists := expectedPoints[id]
		require.True(t, exists)
		require.Equal(t, expected.doc, point.Payload["doc"].GetStringValue())
		require.Equal(t, expected.source, point.Payload["source"].GetStringValue())
		vector := point.Vectors.GetVectors().GetVectors()["vector"].GetDenseVector().GetData()
		require.Equal(t, expected.vector, vector)
	}
}
