package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "qdrant-migration",
	Short: "Migrate data to Qdrant from different sources",
	Long:  `A tool to migrate vectorized data to Qdrant from different sources.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

func Execute(projectVersion, projectBuild string) {
	rootCmd.Version = projectVersion
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number of qdrant-migration",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("qdrant-migration %s (%s)\n", projectVersion, projectBuild)
		},
	},
	)
	if err := rootCmd.Execute(); err != nil {
		_, err := fmt.Fprintln(os.Stderr, err)
		if err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(migrateCmd)
}
