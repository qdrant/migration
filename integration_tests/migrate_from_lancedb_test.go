package integrationtests

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const (
	lanceDBTable             = "documents"
	lanceDBOffsetsCollection = "_lancedb_offsets"
	lanceDBDistance          = "euclid"
)

type lanceDBEntry struct {
	id        string
	vectorDim int
	imageDim  int
}

func TestMigrateFromLanceDB(t *testing.T) {
	ctx := context.Background()

	venvPython, venvDir := setupVenv(t, ctx)
	defer func() {
		_ = os.RemoveAll(venvDir)
	}()

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

	sourceURI, expectedEntries := createLanceDBTable(t, ctx, venvPython)

	args := []string{
		"lancedb",
		fmt.Sprintf("--lancedb.uri=%s", sourceURI),
		fmt.Sprintf("--lancedb.table=%s", lanceDBTable),
		"--lancedb.id-column=id",
		"--lancedb.vector-columns=vector,image",
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.distance-metric=vector=%s;image=%s", lanceDBDistance, lanceDBDistance),
		fmt.Sprintf("--migration.offsets-collection=%s", lanceDBOffsetsCollection),
		"--migration.batch-size=7",
		"--migration.create-collection=true",
	}

	runMigrationBinary(t, args, "VIRTUAL_ENV="+venvDir)
	runMigrationBinary(t, args, "VIRTUAL_ENV="+venvDir)

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   int(qdrantPort.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer client.Close()

	points, err := client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(len(expectedEntries) + 1)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(expectedEntries))

	expectedPoints := make(map[string]lanceDBEntry)
	for _, entry := range expectedEntries {
		expectedPoints[entry.id] = entry
	}

	for _, point := range points {
		id := point.Payload["id"].GetStringValue()

		expected, exists := expectedPoints[id]
		require.True(t, exists, "Point with original ID %q not found in expected entries", id)

		vector := point.Vectors.GetVectors().GetVectors()["vector"].GetDenseVector().GetData()
		require.Len(t, vector, expected.vectorDim)

		image := point.Vectors.GetVectors().GetVectors()["image"].GetDenseVector().GetData()
		require.Len(t, image, expected.imageDim)
	}
}

func createLanceDBTable(t *testing.T, ctx context.Context, pythonPath string) (string, []lanceDBEntry) {
	sourceURI := filepath.Join(t.TempDir(), "source")

	scriptPath := "create_lancedb_table.py"
	cmd := exec.CommandContext(ctx, pythonPath, scriptPath, "seed", "--uri", sourceURI)

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to create LanceDB table: %s", string(output))

	_, err = os.Stat(sourceURI)
	require.NoError(t, err, "LanceDB source directory was not created")

	// Mirrors the fixture: ids doc-0..doc-37 with doc-5 deleted.
	expectedEntries := make([]lanceDBEntry, 0, 37)
	for i := range 38 {
		if i == 5 {
			continue
		}
		expectedEntries = append(expectedEntries, lanceDBEntry{
			id:        fmt.Sprintf("doc-%d", i),
			vectorDim: 3,
			imageDim:  2,
		})
	}

	return sourceURI, expectedEntries
}
