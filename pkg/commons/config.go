package commons

type QdrantConfig struct {
	Collection string `help:"Collection name" required:"true"`
	Url        string `help:"Qdrant gRPC URL" default:"http://localhost:6334"`
	APIKey     string `help:"API key for authentication"`
}

type MigrationConfig struct {
	BatchSize         int    `short:"b" help:"Batch size" default:"50"`
	Restart           bool   `help:"Restart the migration and do not continue from last offset" default:"false"`
	CreateCollection  bool   `short:"c" help:"Create the collection if it does not exist" default:"true"`
	OffsetsCollection string `help:"Collection to store the current migration offset" default:"_migration_offsets"`
	MaxMessageSize    int    `help:"Maximum Qdrant gRPC message size in bytes (default: 33554432 = 32MB)" default:"33554432"`
}

type MilvusConfig struct {
	Url           string   `help:"Source Milvus URL, e.g. https://your-milvus-hostname" required:"true"`
	Collection    string   `help:"Source collection" required:"true"`
	APIKey        string   `help:"Source API key"`
	EnableTLSAuth bool     `help:"Enable TLS Auth for Milvus" default:"false"`
	Username      string   `help:"Milvus username"`
	Password      string   `help:"Milvus password"`
	DBName        string   `help:"Milvus database name"`
	ServerVersion string   `help:"Milvus server version"`
	Partitions    []string `help:"Milvus partition names"`
}

type PineconeConfig struct {
	IndexName   string `required:"true"  help:"Pinecone index name"`
	IndexHost   string `required:"true"  help:"Pinecone index host URL (e.g., https://example-index-12345.svc.region.pinecone.io)"`
	APIKey      string `required:"true"  help:"Pinecone API key for authentication"`
	Namespace   string `help:"Namespace of the partition to migrate"`
	ServiceHost string `help:"Pinecone service host URL. Optional."`
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

type WeaviateConfig struct {
	Host         string   `help:"Host of the Weaviate instance (e.g. 'localhost:8080')" required:"true"`
	Scheme       string   `help:"Scheme of the Weaviate instance (e.g. 'http' or 'https')" default:"http"`
	ClassName    string   `help:"Name of the Weaviate class to migrate" required:"true"`
	AuthType     string   `enum:"none,apiKey,password,client,bearer" help:"Authentication type" default:"none"`
	APIKey       string   `help:"API key for authentication (when AuthType is 'apiKey')"`
	Username     string   `help:"Username for authentication (when AuthType is 'password')"`
	Password     string   `help:"Password for authentication (when AuthType is 'password')"`
	Scopes       []string `help:"Scopes for authentication (when AuthType is 'password' or 'client')"`
	ClientSecret string   `help:"Client secret for authentication (when AuthType is 'client')"`
	Token        string   `help:"Bearer token for authentication (when AuthType is 'bearer')"`
	RefreshToken string   `help:"Refresh token for authentication (when AuthType is 'bearer')"`
	ExpiresIn    uint     `help:"Access token expiration time (when AuthType is 'bearer')"`
	Tenant       string   `help:"Objects belonging to which tenant to migrate"`
}

type RedisConfig struct {
	Index      string `help:"Redis FT index name" required:"true"`
	Addr       string `help:"Redis address in the format host:port" default:"localhost:6379"`
	Protocol   int    `help:"Redis protocol version" default:"2"`
	Password   string `help:"Password to authenticate requests"`
	Username   string `help:"Username to authenticate requests"`
	ClientName string `help:"Will execute the 'CLIENT SETNAME <NAME>' for each connection"`
	DB         int    `help:"Database to be selected after connecting to the server"`
	Network    string `help:"Redis network type" enum:"tcp,unix" default:"tcp"`
}
