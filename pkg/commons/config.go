package commons

type QdrantConfig struct {
	Collection string `help:"Collection name" required:"true"`
	Url        string `help:"Qdrant gRPC URL" default:"http://localhost:6334"`
	APIKey     string `help:"API key for authentication"`
}

type MigrationConfig struct {
	BatchSize            int    `short:"b" help:"Batch size" default:"50"`
	Restart              bool   `help:"Restart the migration and do not continue from last offset" default:"false"`
	CreateCollection     bool   `short:"c" help:"Create the collection if it does not exist" default:"true"`
	EnsurePayloadIndexes bool   `help:"Ensure payload indexes are created" default:"true"`
	OffsetsCollection    string `help:"Collection to store the current migration offset" default:"_migration_offsets"`
}

type MilvusConfig struct {
	Url           string `help:"Source Milvus URL, e.g. https://your-milvus-hostname" required:"true"`
	Collection    string `help:"Source collection" required:"true"`
	APIKey        string `help:"Source API key"`
	EnableTLSAuth bool   `help:"Enable TLS Auth for Milvus" default:"false"`
	Username      string `help:"Milvus username"`
	Password      string `help:"Milvus password"`
	DBName        string `help:"Milvus database name"`
	ServerVersion string `help:"Milvus server version"`
}

type PineconeConfig struct {
	APIKey    string `required:"true"  help:"Pinecone API key for authentication"`
	Host      string `required:"true"  help:"Pinecone index host URL (e.g., https://example-index-12345.svc.region.pinecone.io)"`
	Namespace string `help:"Namespace of the partition to migrate"`
}

type ChromaConfig struct {
	Collection  string `required:"true" help:"Chroma collection name"`
	Url         string `help:"Chroma server URL" default:"http://localhost:8000"`
	Tenant      string `help:"Chroma tenant"`
	AuthType    string `help:"Authentication type" enum:"basic,token,none" default:"none"`
	Username    string `help:"Username for basic authentication"`
	Password    string `help:"Password for basic authentication"`
	Token       string `help:"Token for token authentication"`
	TokenHeader string `help:"Token header for authentication" default:"Authorization"`
	Database    string `help:"Database for Chroma"`
}
