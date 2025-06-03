package integrationtests

import "math/rand"

const (
	testCollectionName = "TestCollection"
	qdrantPort         = "6334"
	qdrantAPIKey       = "00000000"
	totalEntries       = 100
	dimension          = 384
)

func randFloat32Values(n int) []float32 {
	values := make([]float32, n)
	for i := range values {
		values[i] = rand.Float32()
	}
	return values
}
