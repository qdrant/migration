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

	faissIndexPath, expectedEntries := createFaissIndex(t, ctx, venvPython)

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

	runMigrationBinary(t, args, "VIRTUAL_ENV="+venvDir)

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

func createFaissIndex(t *testing.T, ctx context.Context, pythonPath string) (string, []faissEntry) {
	tempDir := t.TempDir()
	faissIndexPath := filepath.Join(tempDir, faissIndexName)

	expectedEntries := make([]faissEntry, totalEntries)
	vectorsData := make([]string, totalEntries)

	for i := range totalEntries {
		vector := make([]float32, dimension)
		for j := range dimension {
			// Keep upto 5 decimal places only to avoid precision issues
			val := float32(int(rand.Float32()*10000)) / 10000.0

			vector[j] = val
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

	cmdArgs := []string{pythonPath, scriptPath, fmt.Sprintf("%d", dimension), fmt.Sprintf("%d", totalEntries), faissIndexPath}
	cmdArgs = append(cmdArgs, vectorsData...)

	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to create FAISS index: %s", string(output))

	_, err = os.Stat(faissIndexPath)
	require.NoError(t, err, "FAISS index file was not created")

	return faissIndexPath, expectedEntries
}

func setupVenv(t *testing.T, ctx context.Context) (string, string) {
	t.Helper()

	baseDir := t.TempDir()
	venvDir := filepath.Join(baseDir, ".venv")

	cmdVenv := exec.CommandContext(ctx, "python3", "-m", "venv", venvDir)
	output, err := cmdVenv.CombinedOutput()
	require.NoError(t, err, "failed to create venv: %s", string(output))

	venvPython := filepath.Join(venvDir, "bin", "python")

	reqPath := filepath.Join("..", "requirements.txt")
	cmdPip := exec.CommandContext(ctx, venvPython, "-m", "pip", "install", "--no-cache-dir", "--no-compile", "--prefer-binary", "-r", reqPath)
	output, err = cmdPip.CombinedOutput()
	require.NoError(t, err, "failed to install requirements: %s", string(output))

	return venvPython, venvDir
}
