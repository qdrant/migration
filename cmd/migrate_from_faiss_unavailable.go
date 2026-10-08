//go:build no_faiss

package cmd

func init() {
	registerUnavailableCommand("faiss", "Migrate data from a FAISS index to Qdrant.")
}
