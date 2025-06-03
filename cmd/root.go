package cmd

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/pterm/pterm"
)

type Globals struct {
	Debug               bool             `help:"Enable debug mode."`
	Trace               bool             `help:"Enable trace mode."`
	SkipTlsVerification bool             `help:"Skip TLS verification."`
	Version             kong.VersionFlag `name:"version" help:"Print version information and quit"`
}

type CLI struct {
	Globals

	Qdrant   MigrateFromQdrantCmd   `cmd:"" help:"Migrate data from a Qdrant database to Qdrant."`
	Milvus   MigrateFromMilvusCmd   `cmd:"" help:"Migrate data from a Milvus database to Qdrant."`
	Pinecone MigrateFromPineconeCmd `cmd:"" help:"Migrate data from a Pinecone database to Qdrant."`
	Chroma   MigrateFromChromaCmd   `cmd:"" help:"Migrate data from a Chroma database to Qdrant."`
	Weaviate MigrateFromWeaviateCmd `cmd:"" help:"Migrate data from a Weaviate database to Qdrant."`
	Redis    MigrateFromRedisCmd    `cmd:"" help:"Migrate data from a Redis database to Qdrant."`
}

func Execute(projectVersion, projectBuild string) {
	version := fmt.Sprintf("Version: %s, Build: %s", projectVersion, projectBuild)
	cli := CLI{}
	ctx := kong.Parse(&cli,
		kong.Name("migration"),
		kong.Description("Migrate data to Qdrant from other sources."),
		kong.Vars{
			"version": version,
		})

	err := ctx.Run(&cli.Globals)

	if err != nil {
		fmt.Print("\n")
		pterm.Error.Println(err)
		ctx.Exit(1)
	}
}

func NewParser(args []string) (*kong.Context, error) {
	cli := &CLI{}

	parser, err := kong.New(cli, kong.Bind(&cli.Globals))
	if err != nil {
		return nil, err
	}

	return parser.Parse(args)
}
