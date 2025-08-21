package integrationtests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const (
	faissIndexName = "test_faiss_index"
	faissDistance  = "euclid"
)

type faissEntry struct {
	id     int
	vector []float32
}

func TestMigrateFromFaiss(t *testing.T) {
	ctx := context.Background()

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

	faissIndexPath, expectedEntries := createFaissIndex(t, ctx)

	args := []string{
		"faiss",
		fmt.Sprintf("--faiss.index-path=%s", faissIndexPath),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.distance-metric=%s", faissDistance),
		"--migration.batch-size=10",
		"--migration.create-collection=true",
	}

	runMigrationBinary(t, args)

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   qdrantPort.Int(),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer client.Close()

	points, err := client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(len(expectedEntries))),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(expectedEntries))

	expectedPoints := make(map[int]faissEntry)
	for _, entry := range expectedEntries {
		expectedPoints[entry.id] = entry
	}

	for _, point := range points {
		id := int(point.Id.GetNum())

		expected, exists := expectedPoints[id]
		require.True(t, exists, "Point with ID %d not found in expected entries", id)

		vector := point.Vectors.GetVector().GetData()
		require.Equal(t, expected.vector, vector)
	}

	os.Remove(faissIndexPath)
}

func createFaissIndex(t *testing.T, ctx context.Context) (string, []faissEntry) {
	tempDir := t.TempDir()
	faissIndexPath := filepath.Join(tempDir, faissIndexName)

	expectedEntries := make([]faissEntry, totalEntries)
	vectorsData := make([]string, totalEntries)

	for i := range totalEntries {
		vector := make([]float32, dimension)
		for j := range dimension {
			// Keep upto 5 decimal places only to avoid precision issues
			val := int(rand.Float32()*100000) / 100000
			vector[j] = float32(val)
		}

		expectedEntries[i] = faissEntry{
			id:     i,
			vector: vector,
		}

		vectorStr := make([]string, dimension)
		for j, val := range vector {
			vectorStr[j] = fmt.Sprintf("%.4f", val)
		}
		vectorsData[i] = strings.Join(vectorStr, ",")
	}

	scriptPath := "create_faiss_index.py"

	cmdArgs := []string{"python3", scriptPath, fmt.Sprintf("%d", dimension), fmt.Sprintf("%d", totalEntries), faissIndexPath}
	cmdArgs = append(cmdArgs, vectorsData...)

	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to create FAISS index: %s", string(output))

	_, err = os.Stat(faissIndexPath)
	require.NoError(t, err, "FAISS index file was not created")

	return faissIndexPath, expectedEntries
}
