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

	Qdrant        MigrateFromQdrantCmd        `cmd:"" help:"Migrate data from a Qdrant database to Qdrant."`
	Milvus        MigrateFromMilvusCmd        `cmd:"" help:"Migrate data from a Milvus database to Qdrant."`
	Pinecone      MigrateFromPineconeCmd      `cmd:"" help:"Migrate data from a Pinecone database to Qdrant."`
	Chroma        MigrateFromChromaCmd        `cmd:"" help:"Migrate data from a Chroma database to Qdrant."`
	Weaviate      MigrateFromWeaviateCmd      `cmd:"" help:"Migrate data from a Weaviate database to Qdrant."`
	Redis         MigrateFromRedisCmd         `cmd:"" help:"Migrate data from a Redis database to Qdrant."`
	Mongodb       MigrateFromMongoDBCmd       `cmd:"" help:"Migrate data from a Mongo database to Qdrant."`
	OpenSearch    MigrateFromOpenSearchCmd    `cmd:"" name:"opensearch" help:"Migrate data from an OpenSearch database to Qdrant."`
	Elasticsearch MigrateFromElasticsearchCmd `cmd:"" help:"Migrate data from an Elasticsearch database to Qdrant."`
	PG            MigrateFromPGCmd            `cmd:"" name:"pg" help:"Migrate data from a PostgreSQL database to Qdrant."`
	S3Vectors     MigrateFromS3VectorsCmd     `cmd:"" name:"s3" help:"Migrate data from S3 Vectors to Qdrant."`
	Faiss         MigrateFromFaissCmd         `cmd:"" help:"Migrate data from a FAISS index to Qdrant."`
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
