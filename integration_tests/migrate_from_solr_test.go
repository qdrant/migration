package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

func TestMigrateFromSolr(t *testing.T) {
	ctx := context.Background()

	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	solrCont := solrContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, solrCont.Terminate(ctx))
	})

	solrHost, err := solrCont.PortEndpoint(ctx, "8983/tcp", "http")
	require.NoError(t, err)

	qdrantHost, err := qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort := mappedPort.Int()

	collectionName := "test-collection"

	schemaURL := fmt.Sprintf("%s/solr/%s/schema", solrHost, collectionName)

	simpleFieldDef := map[string]interface{}{
		"add-field": map[string]interface{}{
			"name":        "vector",
			"type":        "pfloats",
			"indexed":     false,
			"stored":      true,
			"multiValued": true,
		},
	}

	fieldBody, err := json.Marshal(simpleFieldDef)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, schemaURL, bytes.NewReader(fieldBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "Failed to add field: %s", string(body))

	for _, fieldName := range []string{"doc", "source"} {
		fieldDef := map[string]interface{}{
			"add-field": map[string]interface{}{
				"name":   fieldName,
				"type":   "string",
				"stored": true,
			},
		}
		fieldBody, err := json.Marshal(fieldDef)
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, schemaURL, bytes.NewReader(fieldBody))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	testIDs := make([]string, totalEntries)
	testVectors := make([][]float32, totalEntries)
	testDocs := make([]string, totalEntries)
	testSources := make([]string, totalEntries)

	updateURL := fmt.Sprintf("%s/solr/%s/update?commit=true", solrHost, collectionName)
	for i := 0; i < totalEntries; i++ {
		testIDs[i] = fmt.Sprintf("doc-%d", i+1)
		testVectors[i] = randFloat32Values(dimension)
		testDocs[i] = fmt.Sprintf("test doc %d", i+1)
		testSources[i] = fmt.Sprintf("source%d", i+1)

		doc := map[string]interface{}{
			"id":     testIDs[i],
			"vector": testVectors[i],
			"doc":    testDocs[i],
			"source": testSources[i],
		}

		docBody, err := json.Marshal([]map[string]interface{}{doc})
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, updateURL, bytes.NewReader(docBody))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "Failed to index document: %s", string(body))
	}

	time.Sleep(2 * time.Second)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   qdrantPort,
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	err = qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: testCollectionName,
		VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
			"vector": {
				Size:     uint64(dimension),
				Distance: qdrant.Distance_Dot,
			},
		}),
	})
	require.NoError(t, err)

	args := []string{
		"solr",
		fmt.Sprintf("--solr.url=%s", solrHost),
		fmt.Sprintf("--solr.collection=%s", collectionName),
		fmt.Sprintf("--qdrant.url=http://%s:%d", qdrantHost, qdrantPort),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
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
		require.True(t, exists, "Unexpected point ID: %s", id)
		require.Equal(t, expected.doc, point.Payload["doc"].GetStringValue())
		require.Equal(t, expected.source, point.Payload["source"].GetStringValue())
		vector := point.Vectors.GetVectors().GetVectors()["vector"].GetData()
		require.Equal(t, expected.vector, vector)
	}
}
