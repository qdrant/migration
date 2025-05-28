package cmd_test

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"path/filepath"
	"testing"

	chroma "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/amikos-tech/chroma-go/pkg/embeddings"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const (
	chromaPort    = "8000"
	sourceField   = "source"
	documentField = "document"
	distance      = "euclid"
)

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
	qdrantPort, err := qdrantContainer.MappedPort(ctx, qdrantPort)
	require.NoError(t, err)

	chromaClient, err := chroma.NewHTTPClient(chroma.WithBaseURL("http://" + chromaHost + ":" + chromaPort.Port()))
	require.NoError(t, err)

	collection, err := chromaClient.GetOrCreateCollection(ctx, testCollectionName)
	require.NoError(t, err)

	testIDs := make([]chroma.DocumentID, totalEntries)
	testEmbeddings := make([]embeddings.Embedding, totalEntries)
	testDocuments := make([]string, totalEntries)
	testMetadatas := make([]chroma.DocumentMetadata, totalEntries)

	for i := 0; i < totalEntries; i++ {
		testIDs[i] = chroma.DocumentID(fmt.Sprintf("%d", i+1))

		randomVector := make([]float32, dimension)
		for j := range randomVector {
			randomVector[j] = rand.Float32()
		}
		testEmbeddings[i] = embeddings.NewEmbeddingFromFloat32(randomVector)

		testDocuments[i] = fmt.Sprintf("test document %d", i+1)
		meta, err := chroma.NewDocumentMetadataFromMap(map[string]interface{}{
			sourceField: fmt.Sprintf("test%d", i+1),
		})
		require.NoError(t, err)
		testMetadatas[i] = meta
	}

	err = collection.Add(
		ctx,
		chroma.WithTexts(testDocuments...),
		chroma.WithIDs(testIDs...),
		chroma.WithMetadatas(testMetadatas...),
		chroma.WithEmbeddings(testEmbeddings...),
	)
	require.NoError(t, err)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:   qdrantHost,
		Port:   qdrantPort.Int(),
		APIKey: qdrantAPIKey,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	binaryPath := filepath.Join(t.TempDir(), "migration")
	cmd := exec.Command("go", "build", "-o", binaryPath, "main.go")
	cmd.Dir = ".."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build failed: %s", string(out))

	args := []string{
		"chroma",
		fmt.Sprintf("--chroma.url=http://%s:%s", chromaHost, chromaPort.Port()),
		fmt.Sprintf("--chroma.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--migration.batch-size=%d", batchSize),
		fmt.Sprintf("--migration.offsets-collection=%s", offsetsCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		fmt.Sprintf("--qdrant.document-field=%s", documentField),
		fmt.Sprintf("--qdrant.distance=%s", distance),
		fmt.Sprintf("--qdrant.dense-vector=%s", denseVectorField),
	}

	cmd = exec.Command(binaryPath, args...)
	out, err = cmd.CombinedOutput()
	require.NoError(t, err, "migration failed: %s", string(out))

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(len(testIDs))),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(testIDs))

	expectedPoints := make(map[string]struct {
		document string
		source   string
		vector   []float32
	})
	for i, id := range testIDs {
		source, ok := testMetadatas[i].GetString(sourceField)
		require.True(t, ok)
		expectedPoints[string(id)] = struct {
			document string
			source   string
			vector   []float32
		}{
			document: testDocuments[i],
			source:   source,
			vector:   testEmbeddings[i].ContentAsFloat32(),
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		expected, exists := expectedPoints[id]
		require.True(t, exists)

		require.Equal(t, expected.document, point.Payload[documentField].GetStringValue())
		require.Equal(t, expected.source, point.Payload[sourceField].GetStringValue())

		vector := point.Vectors.GetVectors().GetVectors()[denseVectorName].GetData()
		require.Equal(t, expected.vector, vector)
	}
}
