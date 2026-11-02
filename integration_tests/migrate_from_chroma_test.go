package integrationtests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const (
	chromaPort    = "8000"
	documentField = "document"
	uriField      = "uri"
	sourceField   = "source"
	indexField    = "index"
	scoreField    = "score"
	tagsField     = "tags"
)

func chromaPost(t *testing.T, url string, body, out any) {
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Contains(t, []int{http.StatusOK, http.StatusCreated}, resp.StatusCode)

	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	}
}

func TestMigrateFromChroma(t *testing.T) {
	ctx := context.Background()

	qdrantContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		if err := qdrantContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Qdrant container: %v", err)
		}
	}()
	chromaContainer := chromaContainer(ctx, t)
	defer func() {
		if err := chromaContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Chroma container: %v", err)
		}
	}()

	chromaHost, err := chromaContainer.Host(ctx)
	require.NoError(t, err)
	chromaPort, err := chromaContainer.MappedPort(ctx, chromaPort)
	require.NoError(t, err)

	qdrantHost, err := qdrantContainer.Host(ctx)
	require.NoError(t, err)
	qdrantPort, err := qdrantContainer.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)

	chromaURL := fmt.Sprintf("http://%s:%s", chromaHost, chromaPort.Port())
	collectionsURL := chromaURL + "/api/v2/tenants/default_tenant/databases/default_database/collections"

	var collection struct {
		ID string `json:"id"`
	}
	chromaPost(t, collectionsURL, map[string]any{
		"name":          testCollectionName,
		"configuration": map[string]any{"hnsw": map[string]any{"space": "ip"}},
	}, &collection)

	ids := make([]string, totalEntries)
	vectors := make([][]float32, totalEntries)
	documents := make([]*string, totalEntries)
	uris := make([]*string, totalEntries)
	metadatas := make([]map[string]any, totalEntries)

	for i := range totalEntries {
		ids[i] = fmt.Sprintf("%d", i+1)
		vectors[i] = randFloat32Values(dimension)
		if i%3 != 0 {
			documents[i] = qdrant.PtrOf(fmt.Sprintf("test document %d", i+1))
		}
		if i%2 == 0 {
			uris[i] = qdrant.PtrOf(fmt.Sprintf("s3://bucket/%d.png", i+1))
		}
		metadatas[i] = map[string]any{
			sourceField: fmt.Sprintf("test%d", i+1),
			indexField:  i,
			scoreField:  float64(i) + 0.5,
			tagsField:   []string{"tag", fmt.Sprintf("tag%d", i+1)},
		}
	}

	chromaPost(t, collectionsURL+"/"+collection.ID+"/add", map[string]any{
		"ids":        ids,
		"embeddings": vectors,
		"documents":  documents,
		"uris":       uris,
		"metadatas":  metadatas,
	}, nil)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   int(qdrantPort.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	args := []string{
		"chroma",
		fmt.Sprintf("--chroma.url=%s", chromaURL),
		fmt.Sprintf("--chroma.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		fmt.Sprintf("--qdrant.document-field=%s", documentField),
		fmt.Sprintf("--qdrant.uri-field=%s", uriField),
	}

	runMigrationBinary(t, args)

	info, err := qdrantClient.GetCollectionInfo(ctx, testCollectionName)
	require.NoError(t, err)
	require.Equal(t, qdrant.Distance_Dot, info.GetConfig().GetParams().GetVectorsConfig().GetParams().GetDistance())

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(totalEntries)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, totalEntries)

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		var i int
		_, err := fmt.Sscanf(id, "%d", &i)
		require.NoError(t, err)
		i--

		expectedPayload := map[string]*qdrant.Value{
			idField:     qdrant.NewValueString(ids[i]),
			sourceField: qdrant.NewValueString(fmt.Sprintf("test%d", i+1)),
			indexField:  qdrant.NewValueInt(int64(i)),
			scoreField:  qdrant.NewValueDouble(float64(i) + 0.5),
			tagsField:   qdrant.NewValueFromList(qdrant.NewValueString("tag"), qdrant.NewValueString(fmt.Sprintf("tag%d", i+1))),
		}
		if documents[i] != nil {
			expectedPayload[documentField] = qdrant.NewValueString(*documents[i])
		}
		if uris[i] != nil {
			expectedPayload[uriField] = qdrant.NewValueString(*uris[i])
		}
		require.Equal(t, expectedPayload, point.Payload)
		require.Equal(t, vectors[i], point.Vectors.GetVector().GetDenseVector().GetData())
	}

	count, err := qdrantClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: testCollectionName,
		Filter: &qdrant.Filter{
			Must: []*qdrant.Condition{qdrant.NewMatchInt(indexField, 7)},
		},
		Exact: qdrant.PtrOf(true),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
}
