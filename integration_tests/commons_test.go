package integrationtests

import (
	"math/rand"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testCollectionName = "TestCollection"
	qdrantGRPCPort     = "6334"
	qdrantAPIKey       = "00000000"
	totalEntries       = 100
	dimension          = 384
	idField            = "__id__"
	denseVectorField   = "dense_vector"
)

func randFloat32Values(n int) []float32 {
	values := make([]float32, n)
	for i := range values {
		values[i] = rand.Float32()
	}
	return values
}

func randIndices(n int) []uint32 {
	indices := make([]uint32, n)
	for i := range indices {
		indices[i] = rand.Uint32()
	}
	return indices
}

func runMigrationBinary(t *testing.T, args []string) {
	binaryPath := filepath.Join(t.TempDir(), "migration")
	cmd := exec.Command("go", "build", "-o", binaryPath, "main.go")
	cmd.Dir = ".."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build failed: %s", string(out))

	cmd = exec.Command(binaryPath, args...)
	out, err = cmd.CombinedOutput()
	require.NoError(t, err, "migration failed: %s", string(out))
}
