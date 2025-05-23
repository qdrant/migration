package cmd_test

import (
	"context"
	"testing"

	"github.com/Anush008/chroma-go/pkg/embeddings"
	chroma "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/migration/cmd"
	"github.com/qdrant/migration/pkg/commons"
)

func TestMigrateFromChroma(t *testing.T) {
	ctx := context.Background()

	// Start Chroma container
	chromaContainer, err := chromaContainer(ctx)
	require.NoError(t, err)
	defer chromaContainer.Terminate(ctx)

	qdrantContainer, err := qdrantContainer(ctx, "<CHROMA_MIGRATION_API_KEY>")
	require.NoError(t, err)
	defer qdrantContainer.Terminate(ctx)

	chromaHost, err := chromaContainer.Host(ctx)
	require.NoError(t, err)
	chromaPort, err := chromaContainer.MappedPort(ctx, "8000")
	require.NoError(t, err)

	qdrantHost, err := qdrantContainer.Host(ctx)
	require.NoError(t, err)
	qdrantPort, err := qdrantContainer.MappedPort(ctx, "6334")
	require.NoError(t, err)

	chromaClient, err := chroma.NewHTTPClient(chroma.WithBaseURL("http://" + chromaHost + ":" + chromaPort.Port()))
	require.NoError(t, err)

	collection, err := chromaClient.GetOrCreateCollection(ctx, "test_collection")
	require.NoError(t, err)

	testIDs := []chroma.DocumentID{chroma.DocumentID("1"), chroma.DocumentID("2"), chroma.DocumentID("3")}
	testEmbeddings := []embeddings.Embedding{
		embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}),
		embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}),
		embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0}),
	}

	testDocuments := []string{
		"test document 1",
		"test document 2",
		"test document 3",
	}

	meta1, err := chroma.NewDocumentMetadataFromMap(map[string]interface{}{"source": "test1"})
	require.NoError(t, err)
	meta2, err := chroma.NewDocumentMetadataFromMap(map[string]interface{}{"source": "test2"})
	require.NoError(t, err)
	meta3, err := chroma.NewDocumentMetadataFromMap(map[string]interface{}{"source": "test3"})
	require.NoError(t, err)
	testMetadatas := []chroma.DocumentMetadata{
		meta1,
		meta2,
		meta3,
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
		APIKey: "<CHROMA_MIGRATION_API_KEY>",
	})
	require.NoError(t, err)

	// Create migration command
	migrationCmd := &cmd.MigrateFromChromaCmd{
		Chroma: commons.ChromaConfig{
			Url:        "http://" + chromaHost + ":" + chromaPort.Port(),
			Collection: "test_collection",
		},
		Qdrant: commons.QdrantConfig{
			Url:        "http://" + qdrantHost + ":" + qdrantPort.Port(),
			Collection: "test_collection",
		},
		Migration: commons.MigrationConfig{
			BatchSize:        100,
			CreateCollection: true,
		},
		IdField:       "__id__",
		DocumentField: "document",
	}

	err = migrationCmd.Run(&cmd.Globals{})
	require.NoError(t, err)

	count, err := qdrantClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: "test_collection",
		Exact:          qdrant.PtrOf(true),
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(len(testIDs)), count)

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: "test_collection",
		Limit:          qdrant.PtrOf(uint32(len(testIDs))),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(testIDs))

	for i, point := range points {
		assert.Equal(t, string(testIDs[i]), point.Payload["__id__"].GetStringValue())

		payload := point.Payload

		doc := payload["document"].GetStringValue()
		assert.Equal(t, testDocuments[i], doc)

		metadata := payload["source"].GetStringValue()
		source, ok := testMetadatas[i].GetString("source")
		require.True(t, ok)
		assert.Equal(t, source, metadata)

		vector := point.Vectors.GetVectors().Vectors["dense_vector"].GetData()
		assert.Equal(t, testEmbeddings[i], vector)
	}
}
