//go:build no_chroma

package cmd

func init() {
	registerUnavailableCommand("chroma", "Migrate data from a Chroma database to Qdrant.")
}
