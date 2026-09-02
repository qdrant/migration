package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const azureAPIVersion = "2026-04-01"

func azureRequest(t *testing.T, endpoint, apiKey, method, path string, body any) (int, string) {
	t.Helper()

	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequest(method, endpoint+path+"?api-version="+azureAPIVersion, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("api-key", apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var buf bytes.Buffer
	_, err = buf.ReadFrom(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, buf.String()
}

func TestMigrateFromAzureSearch(t *testing.T) {
	endpoint := os.Getenv("AZURE_SEARCH_ENDPOINT")
	apiKey := os.Getenv("AZURE_SEARCH_API_KEY")
	if endpoint == "" || apiKey == "" {
		t.Skip("Skipping Azure AI Search test. AZURE_SEARCH_ENDPOINT and AZURE_SEARCH_API_KEY not set")
	}

	ctx := context.Background()

	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
	})

	qdrantHost, err := qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort := mappedPort.Num()

	indexName := fmt.Sprintf("qdrant-migration-test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		status, body := azureRequest(t, endpoint, apiKey, http.MethodDelete, "/indexes/"+indexName, nil)
		require.Equal(t, http.StatusNoContent, status, "failed to delete index: %s", body)
	})

	indexDef := map[string]any{
		"name": indexName,
		"fields": []any{
			map[string]any{"name": "id", "type": "Edm.String", "key": true, "sortable": true, "filterable": true, "retrievable": true},
			map[string]any{"name": "title", "type": "Edm.String", "searchable": true, "retrievable": true},
			map[string]any{"name": "content", "type": "Edm.String", "searchable": true, "retrievable": true},
			map[string]any{"name": "vector", "type": "Collection(Edm.Single)", "dimensions": dimension, "retrievable": true, "searchable": true, "vectorSearchProfile": "my-profile"},
		},
		"vectorSearch": map[string]any{
			"algorithms": []any{
				map[string]any{"name": "hnsw-1", "kind": "hnsw", "hnswParameters": map[string]any{"metric": "dotProduct"}},
			},
			"profiles": []any{
				map[string]any{"name": "my-profile", "algorithm": "hnsw-1"},
			},
		},
	}

	status, body := azureRequest(t, endpoint, apiKey, http.MethodPost, "/indexes", indexDef)
	require.Equal(t, http.StatusCreated, status, "failed to create index: %s", body)

	testIDs := make([]string, totalEntries)
	testTitles := make([]string, totalEntries)
	testContents := make([]string, totalEntries)
	testVectors := make([][]float32, totalEntries)

	docs := make([]any, 0, totalEntries)
	for i := 0; i < totalEntries; i++ {
		testIDs[i] = fmt.Sprintf("doc-%d", i+1)
		testTitles[i] = fmt.Sprintf("title-%d", i+1)
		testContents[i] = fmt.Sprintf("content-%d", i+1)
		testVectors[i] = randFloat32Values(dimension)
		docs = append(docs, map[string]any{
			"@search.action": "upload",
			"id":             testIDs[i],
			"title":          testTitles[i],
			"content":        testContents[i],
			"vector":         testVectors[i],
		})
	}

	status, body = azureRequest(t, endpoint, apiKey, http.MethodPost, "/indexes/"+indexName+"/docs/index", map[string]any{"value": docs})
	require.Equal(t, http.StatusOK, status, "failed to index documents: %s", body)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   int(qdrantPort),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	args := []string{
		"azure",
		fmt.Sprintf("--azure.endpoint=%s", endpoint),
		fmt.Sprintf("--azure.api-key=%s", apiKey),
		fmt.Sprintf("--azure.index=%s", indexName),
		fmt.Sprintf("--qdrant.url=http://%s:%d", qdrantHost, qdrantPort),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		"--migration.batch-size=10",
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

	expected := make(map[string]struct {
		title   string
		content string
		vector  []float32
	})
	for i := 0; i < totalEntries; i++ {
		expected[testIDs[i]] = struct {
			title   string
			content string
			vector  []float32
		}{
			title:   testTitles[i],
			content: testContents[i],
			vector:  testVectors[i],
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		exp, ok := expected[id]
		require.True(t, ok)
		require.Equal(t, exp.title, point.Payload["title"].GetStringValue())
		require.Equal(t, exp.content, point.Payload["content"].GetStringValue())
		vector := point.Vectors.GetVectors().GetVectors()["vector"].GetDenseVector().GetData()
		require.Equal(t, exp.vector, vector)
	}
}
