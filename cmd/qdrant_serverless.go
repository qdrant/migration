package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/qdrant/go-client/qdrant"
	"github.com/qdrant/go-client/qdrant/serverless"

	"github.com/qdrant/migration/pkg/commons"
)

// qdrantPoints holds the point operations that *qdrant.Client and *serverless.Client share.
type qdrantPoints interface {
	commons.PointsClient
	pointCounter
	Query(ctx context.Context, request *qdrant.QueryPoints) ([]*qdrant.ScoredPoint, error)
	GetPointsClient() qdrant.PointsClient
	Close() error
}

// qdrantConnection is a connection to a Qdrant cluster or to a Qdrant Serverless space. Exactly one of cluster and serverless is set.
// Point operations go through the embedded qdrantPoints. Collection operations need the concrete client.
type qdrantConnection struct {
	qdrantPoints
	cluster    *qdrant.Client
	serverless *serverless.Client
}

func connectToQdrantOrServerless(globals *Globals, host string, port int, apiKey string, useTLS, isServerless bool, maxMessageSize int) (*qdrantConnection, error) {
	if !isServerless {
		client, err := connectToQdrant(globals, host, port, apiKey, useTLS, maxMessageSize)
		if err != nil {
			return nil, err
		}
		return &qdrantConnection{qdrantPoints: client, cluster: client}, nil
	}

	// The serverless client always connects over TLS on port 443.
	client, err := serverless.NewClient(&qdrant.Config{
		Host:        host,
		APIKey:      apiKey,
		TLSConfig:   &tls.Config{InsecureSkipVerify: globals.SkipTlsVerification},
		GrpcOptions: append(grpcDialOptions(globals, maxMessageSize), grpc.WithChainUnaryInterceptor(retryServerlessPointRequests())),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create serverless client: %w", err)
	}
	return &qdrantConnection{qdrantPoints: client, serverless: client}, nil
}

// retryServerlessPointRequests retries a point request that fails with a temporary error,
// such as "no healthy upstream" from Qdrant Serverless or a reset stream. A point request of a migration gives the same result when it runs again.
// Collection requests are not retried, because their results change after the first attempt.
func retryServerlessPointRequests() grpc.UnaryClientInterceptor {
	retryInterceptor := retry.UnaryClientInterceptor(
		retry.WithRetriable(isTemporaryServerlessError),
		retry.WithMax(serverlessRetryAttempts),
		retry.WithBackoff(retry.BackoffExponentialWithJitterBounded(200*time.Millisecond, 0.2, 5*time.Second)),
	)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if !strings.HasPrefix(method, "/qdrant.Points/") {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		return retryInterceptor(ctx, method, req, reply, cc, invoker, opts...)
	}
}

func isTemporaryServerlessError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted:
		return true
	case codes.Internal:
		return strings.Contains(err.Error(), "RST_STREAM")
	}
	return false
}

func (c *qdrantConnection) CollectionExists(ctx context.Context, collection string) (bool, error) {
	if c.serverless != nil {
		return c.serverless.CollectionExists(ctx, collection)
	}
	return c.cluster.CollectionExists(ctx, collection)
}

// CreateCollection creates a collection. In a Qdrant Serverless space, only the vectors of the request carry over.
// As in a Qdrant cluster, it fails if the collection exists.
func (c *qdrantConnection) CreateCollection(ctx context.Context, request *qdrant.CreateCollection) error {
	if c.serverless == nil {
		return c.cluster.CreateCollection(ctx, request)
	}
	name := request.GetCollectionName()
	config, err := serverlessVectorsConfig(request.GetVectorsConfig(), request.GetSparseVectorsConfig())
	if err != nil {
		return err
	}
	created, err := createServerlessCollection(ctx, c.serverless, name, config)
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("collection %q already exists", name)
	}
	// The callers look the collection up before they create it. After such a lookup,
	// Qdrant Serverless can report the new collection as missing for a few seconds.
	exists, err := retryWhileServerlessReportsMissing(ctx, func() (bool, error) {
		return c.serverless.CollectionExists(ctx, name)
	})
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("qdrant serverless reports the new collection %q as missing", name)
	}
	return nil
}

func (c *qdrantConnection) prepareOffsetsCollection(ctx context.Context, name string) error {
	if c.serverless == nil {
		return commons.PrepareOffsetsCollection(ctx, name, c.cluster)
	}
	// Qdrant Serverless requires at least one vector in a collection. The offset points do not use it.
	_, err := createServerlessCollection(ctx, c.serverless, name, &serverless.CollectionConfig{
		SparseVectors: map[string]*serverless.SparseVectorConfig{"offset": {}},
	})
	return err
}

func (c *qdrantConnection) deleteOffsetsCollection(ctx context.Context, name string) error {
	if c.serverless == nil {
		return commons.DeleteOffsetsCollection(ctx, name, c.cluster)
	}
	deleted, err := retryWhileServerlessReportsMissing(ctx, func() (bool, error) {
		resp, err := c.serverless.DeleteCollection(ctx, name)
		return resp.GetDeleted(), err
	})
	if err != nil {
		return err
	}
	// This run created the collection, so a deletion that reports it as missing has failed.
	// A collection left behind would make the next run skip the points before the stored offsets.
	if !deleted {
		return fmt.Errorf("qdrant serverless reports offsets collection %q as missing, delete it before the next migration", name)
	}
	// For a few seconds after a deletion, Qdrant Serverless can fail to create a collection with the same name,
	// or lose the writes to it. The wait protects the next migration, which creates this collection again.
	select {
	case <-ctx.Done():
	case <-time.After(serverlessRecreateDelay):
	}
	return nil
}

// countPoints counts the points of a collection after a migration that wrote the expected number of points.
// It waits for the writes to show. Reads from Qdrant Serverless lag behind writes,
// and a Qdrant cluster applies the writes of the parallel workers after it accepts them.
func (c *qdrantConnection) countPoints(ctx context.Context, collection string, expected uint64) (uint64, error) {
	if c.serverless != nil {
		return waitForPointCount(ctx, c, collection, expected, serverlessCountLag)
	}
	return waitForPointCount(ctx, c, collection, expected, clusterCountLag)
}

// createServerlessCollection creates a collection in a Qdrant Serverless space unless it exists,
// and reports whether it created the collection.
// It does not look the collection up first, because after a lookup of a missing collection,
// Qdrant Serverless can report the collection as missing for a few seconds after it is created.
func createServerlessCollection(ctx context.Context, client *serverless.Client, collection string, config *serverless.CollectionConfig) (bool, error) {
	if err := validateServerlessVectorNames(config); err != nil {
		return false, err
	}
	resp, err := client.CreateCollection(ctx, collection, config)
	// A collection that exists already is reported as an AlreadyExists error, or through the result.
	if status.Code(err) == codes.AlreadyExists {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return resp.GetResult() == "created", nil
}

// serverlessVectorNameChars are the characters that a vector name in Qdrant Serverless can contain.
// Qdrant Serverless creates a collection with other characters in a vector name, but then it cannot store the vectors.
const serverlessVectorNameChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./&:@!$'(),;="

func validateServerlessVectorNames(config *serverless.CollectionConfig) error {
	names := make([]string, 0, len(config.GetDenseVectors())+len(config.GetSparseVectors()))
	for name := range config.GetDenseVectors() {
		names = append(names, name)
	}
	for name := range config.GetSparseVectors() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if i := strings.IndexFunc(name, func(r rune) bool { return !strings.ContainsRune(serverlessVectorNameChars, r) }); i >= 0 {
			return fmt.Errorf("vector name %q contains %q: a vector name in Qdrant Serverless can contain only ASCII letters, digits, and the characters %s",
				name, []rune(name[i:])[0], serverlessVectorNameChars[62:])
		}
	}
	return nil
}

const (
	serverlessRetryAttempts = 10
	serverlessRetryInterval = time.Second
	// serverlessMaxLimit is the highest number of points that Qdrant Serverless returns for one request.
	serverlessMaxLimit = 1000
)

// retryWhileServerlessReportsMissing calls try until it succeeds, fails, or runs out of attempts.
// Qdrant Serverless can report a recently created collection as missing for a few seconds.
func retryWhileServerlessReportsMissing(ctx context.Context, try func() (bool, error)) (bool, error) {
	for attempt := 1; ; attempt++ {
		ok, err := try()
		if ok || err != nil || attempt == serverlessRetryAttempts {
			return ok, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(serverlessRetryInterval):
		}
	}
}

// getServerlessCollectionConfig returns the configuration of an existing serverless collection.
func getServerlessCollectionConfig(ctx context.Context, client *serverless.Client, collection string) (*serverless.CollectionConfig, error) {
	var resp *serverless.GetCollectionResponse
	exists, err := retryWhileServerlessReportsMissing(ctx, func() (bool, error) {
		var err error
		resp, err = client.GetCollection(ctx, collection)
		return resp.GetExists(), err
	})
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("collection %q does not exist", collection)
	}
	if resp.GetConfig() == nil {
		return nil, fmt.Errorf("collection %q has no configuration", collection)
	}
	return resp.GetConfig(), nil
}

var serverlessDistances = map[qdrant.Distance]serverless.Distance{
	qdrant.Distance_Cosine:    serverless.Distance_COSINE,
	qdrant.Distance_Euclid:    serverless.Distance_EUCLID,
	qdrant.Distance_Dot:       serverless.Distance_DOT,
	qdrant.Distance_Manhattan: serverless.Distance_MANHATTAN,
}

var clusterDistances = invert(serverlessDistances)

var serverlessTokenizers = map[qdrant.TokenizerType]serverless.Tokenizer{
	qdrant.TokenizerType_Prefix:       serverless.Tokenizer_PREFIX,
	qdrant.TokenizerType_Whitespace:   serverless.Tokenizer_WHITESPACE,
	qdrant.TokenizerType_Word:         serverless.Tokenizer_WORD,
	qdrant.TokenizerType_Multilingual: serverless.Tokenizer_MULTILINGUAL,
}

var clusterTokenizers = invert(serverlessTokenizers)

func invert[K, V comparable](m map[K]V) map[V]K {
	inverted := make(map[V]K, len(m))
	for k, v := range m {
		inverted[v] = k
	}
	return inverted
}

// serverlessConfigFromCollectionInfo converts the configuration of a Qdrant cluster collection
// into a serverless collection configuration. Payload indexes that serverless does not support are skipped.
// Cluster-only settings, such as HNSW, quantization, sharding, and replication, are not carried over.
func serverlessConfigFromCollectionInfo(info *qdrant.CollectionInfo) (*serverless.CollectionConfig, error) {
	params := info.GetConfig().GetParams()
	config, err := serverlessVectorsConfig(params.GetVectorsConfig(), params.GetSparseVectorsConfig())
	if err != nil {
		return nil, err
	}

	for field, schema := range info.GetPayloadSchema() {
		if index := serverlessPayloadIndex(schema); index != nil {
			config.PayloadIndexes[field] = index
		}
	}
	return config, nil
}

// serverlessVectorsConfig converts the dense and sparse vectors of a Qdrant cluster collection
// into a serverless collection configuration without payload indexes.
func serverlessVectorsConfig(vectors *qdrant.VectorsConfig, sparseVectors *qdrant.SparseVectorConfig) (*serverless.CollectionConfig, error) {
	config := &serverless.CollectionConfig{
		DenseVectors:   map[string]*serverless.DenseVectorConfig{},
		SparseVectors:  map[string]*serverless.SparseVectorConfig{},
		PayloadIndexes: map[string]*serverless.PayloadIndexConfig{},
	}

	denseVectors := vectors.GetParamsMap().GetMap()
	if vector := vectors.GetParams(); vector != nil {
		// The empty name is the unnamed default vector.
		denseVectors = map[string]*qdrant.VectorParams{"": vector}
	}
	for name, vector := range denseVectors {
		distance, ok := serverlessDistances[vector.GetDistance()]
		if !ok {
			return nil, fmt.Errorf("vector %q uses distance %s, which Qdrant Serverless does not support", name, vector.GetDistance())
		}
		config.DenseVectors[name] = &serverless.DenseVectorConfig{
			Size:        vector.GetSize(),
			Distance:    distance,
			Multivector: vector.GetMultivectorConfig() != nil,
		}
	}

	for name, vector := range sparseVectors.GetMap() {
		config.SparseVectors[name] = &serverless.SparseVectorConfig{
			UseIdf: vector.GetModifier() == qdrant.Modifier_Idf,
		}
	}
	return config, nil
}

// serverlessPayloadIndex converts a Qdrant cluster payload index into a serverless payload index.
// It returns nil for an index type that serverless does not support.
func serverlessPayloadIndex(schema *qdrant.PayloadSchemaInfo) *serverless.PayloadIndexConfig {
	params := schema.GetParams()
	switch schema.GetDataType() {
	case qdrant.PayloadSchemaType_Keyword:
		index := &serverless.KeywordIndex{}
		if params.GetKeywordIndexParams().GetPrefix() != nil {
			index.Prefix = &serverless.KeywordPrefixParams{}
		}
		return serverless.NewPayloadIndexConfigKeyword(index)
	case qdrant.PayloadSchemaType_Integer:
		index := &serverless.IntegerIndex{}
		if integerParams := params.GetIntegerIndexParams(); integerParams != nil {
			index.Lookup = integerParams.Lookup
			index.Range = integerParams.Range
		}
		return serverless.NewPayloadIndexConfigInteger(index)
	case qdrant.PayloadSchemaType_Float:
		return serverless.NewPayloadIndexConfigFloat()
	case qdrant.PayloadSchemaType_Uuid:
		return serverless.NewPayloadIndexConfigUUID()
	case qdrant.PayloadSchemaType_Datetime:
		return serverless.NewPayloadIndexConfigDatetime()
	case qdrant.PayloadSchemaType_Text:
		return serverless.NewPayloadIndexConfigText(serverlessTextIndex(params.GetTextIndexParams()))
	case qdrant.PayloadSchemaType_Geo:
		return serverless.NewPayloadIndexConfigGeo()
	case qdrant.PayloadSchemaType_Bool:
		return serverless.NewPayloadIndexConfigBool()
	}
	return nil
}

func serverlessTextIndex(params *qdrant.TextIndexParams) *serverless.TextIndex {
	// A Qdrant cluster defaults to the word tokenizer, and Qdrant Serverless defaults to the whitespace tokenizer.
	index := &serverless.TextIndex{Tokenizer: serverless.Tokenizer_WORD.Enum()}
	if params == nil {
		return index
	}
	index.Lowercase = params.Lowercase
	index.PhraseMatching = params.PhraseMatching
	index.MinTokenLen = params.MinTokenLen
	index.MaxTokenLen = params.MaxTokenLen
	index.AsciiFolding = params.AsciiFolding
	if tokenizer, ok := serverlessTokenizers[params.GetTokenizer()]; ok {
		index.Tokenizer = tokenizer.Enum()
	}
	if stopwords := params.GetStopwords(); stopwords != nil {
		index.Stopwords = &serverless.StopwordsSet{Languages: stopwords.GetLanguages(), Custom: stopwords.GetCustom()}
	}
	switch stemmer := params.GetStemmer().GetStemmingParams().(type) {
	case *qdrant.StemmingAlgorithm_Snowball:
		index.Stemmer = serverless.NewStemmingAlgorithmSnowball(stemmer.Snowball.GetLanguage())
	case *qdrant.StemmingAlgorithm_Disabled:
		index.Stemmer = serverless.NewStemmingAlgorithmDisabled()
	}
	return index
}

// createCollectionFromServerless converts a serverless collection configuration into a request
// that creates the same vectors on a Qdrant cluster. Precision tiers are not carried over.
// The payload indexes are created separately, see payloadSchemaFromServerless.
func createCollectionFromServerless(collection string, config *serverless.CollectionConfig) (*qdrant.CreateCollection, error) {
	request := &qdrant.CreateCollection{CollectionName: collection}

	denseVectors := make(map[string]*qdrant.VectorParams, len(config.GetDenseVectors()))
	for name, vector := range config.GetDenseVectors() {
		distance, ok := clusterDistances[vector.GetDistance()]
		if !ok {
			return nil, fmt.Errorf("vector %q uses unsupported distance %s", name, vector.GetDistance())
		}
		params := &qdrant.VectorParams{Size: vector.GetSize(), Distance: distance}
		if vector.GetMultivector() {
			params.MultivectorConfig = &qdrant.MultiVectorConfig{Comparator: qdrant.MultiVectorComparator_MaxSim}
		}
		denseVectors[name] = params
	}
	if unnamed, ok := denseVectors[""]; ok && len(denseVectors) == 1 {
		request.VectorsConfig = qdrant.NewVectorsConfig(unnamed)
	} else if len(denseVectors) > 0 {
		request.VectorsConfig = qdrant.NewVectorsConfigMap(denseVectors)
	}

	if len(config.GetSparseVectors()) > 0 {
		sparseVectors := make(map[string]*qdrant.SparseVectorParams, len(config.GetSparseVectors()))
		for name, vector := range config.GetSparseVectors() {
			params := &qdrant.SparseVectorParams{}
			if vector.GetUseIdf() {
				params.Modifier = qdrant.Modifier_Idf.Enum()
			}
			sparseVectors[name] = params
		}
		request.SparseVectorsConfig = qdrant.NewSparseVectorsConfig(sparseVectors)
	}
	return request, nil
}

// payloadSchemaFromServerless converts serverless payload indexes into the payload schema of a Qdrant cluster collection.
func payloadSchemaFromServerless(indexes map[string]*serverless.PayloadIndexConfig) map[string]*qdrant.PayloadSchemaInfo {
	schema := make(map[string]*qdrant.PayloadSchemaInfo, len(indexes))
	for field, index := range indexes {
		if info := clusterPayloadSchema(index); info != nil {
			schema[field] = info
		}
	}
	return schema
}

func clusterPayloadSchema(index *serverless.PayloadIndexConfig) *qdrant.PayloadSchemaInfo {
	switch index := index.GetIndex().(type) {
	case *serverless.PayloadIndexConfig_Keyword:
		params := &qdrant.KeywordIndexParams{}
		if index.Keyword.GetPrefix() != nil {
			params.Prefix = &qdrant.KeywordPrefixParams{}
		}
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Keyword, Params: qdrant.NewPayloadIndexParamsKeyword(params)}
	case *serverless.PayloadIndexConfig_Integer:
		params := &qdrant.IntegerIndexParams{Lookup: index.Integer.Lookup, Range: index.Integer.Range}
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Integer, Params: qdrant.NewPayloadIndexParamsInt(params)}
	case *serverless.PayloadIndexConfig_Float:
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Float}
	case *serverless.PayloadIndexConfig_Uuid:
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Uuid}
	case *serverless.PayloadIndexConfig_Datetime:
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Datetime}
	case *serverless.PayloadIndexConfig_Text:
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Text, Params: qdrant.NewPayloadIndexParamsText(clusterTextIndex(index.Text))}
	case *serverless.PayloadIndexConfig_Geo:
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Geo}
	case *serverless.PayloadIndexConfig_Bool:
		return &qdrant.PayloadSchemaInfo{DataType: qdrant.PayloadSchemaType_Bool}
	}
	return nil
}

func clusterTextIndex(index *serverless.TextIndex) *qdrant.TextIndexParams {
	params := &qdrant.TextIndexParams{
		// Qdrant Serverless defaults to the whitespace tokenizer.
		Tokenizer:      qdrant.TokenizerType_Whitespace,
		Lowercase:      index.Lowercase,
		PhraseMatching: index.PhraseMatching,
		MinTokenLen:    index.MinTokenLen,
		MaxTokenLen:    index.MaxTokenLen,
		AsciiFolding:   index.AsciiFolding,
	}
	if tokenizer, ok := clusterTokenizers[index.GetTokenizer()]; ok {
		params.Tokenizer = tokenizer
	}
	if stopwords := index.GetStopwords(); stopwords != nil {
		params.Stopwords = &qdrant.StopwordsSet{Languages: stopwords.GetLanguages(), Custom: stopwords.GetCustom()}
	}
	switch stemmer := index.GetStemmer().GetStemmingParams().(type) {
	case *serverless.StemmingAlgorithm_Snowball:
		params.Stemmer = qdrant.NewStemmingAlgorithmSnowball(&qdrant.SnowballParams{Language: stemmer.Snowball.GetLanguage()})
	case *serverless.StemmingAlgorithm_Disabled:
		params.Stemmer = qdrant.NewStemmingAlgorithmDisabled()
	}
	return params
}

// These are variables so that tests can shorten them.
var (
	// serverlessCountLag and clusterCountLag are how long the point count of a collection may stay unchanged
	// while the writes of a migration are still on their way. A Qdrant cluster applies the writes that it accepted within seconds.
	serverlessCountLag = time.Minute
	clusterCountLag    = 10 * time.Second
	// pointCountInterval is the time between two point counts.
	pointCountInterval = time.Second
	// serverlessRecreateDelay is how long the tool waits after it deletes a collection,
	// before a collection with the same name can be created again.
	serverlessRecreateDelay = 5 * time.Second
)

// waitForPointCount waits until a collection has at least the expected number of points,
// and returns the highest point count it saw. A large migration can leave a backlog of writes.
// The wait goes on while the point count grows, and stops when it has not grown for the lag.
// A count that fails with a transient error is retried. That error returns only if no count succeeded.
func waitForPointCount(ctx context.Context, client pointCounter, collection string, expected uint64, lag time.Duration) (uint64, error) {
	var highestCount uint64
	var lastErr error
	counted := false
	deadline := time.Now().Add(lag)
	for {
		count, err := client.Count(ctx, &qdrant.CountPoints{
			CollectionName: collection,
			Exact:          qdrant.PtrOf(true),
		})
		if err != nil {
			if !isTransientCountError(err) {
				return 0, err
			}
			lastErr = err
		} else {
			if !counted || count > highestCount {
				deadline = time.Now().Add(lag)
			}
			counted = true
			highestCount = max(highestCount, count)
			if highestCount >= expected {
				return highestCount, nil
			}
		}
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return highestCount, ctx.Err()
		case <-time.After(pointCountInterval):
		}
	}
	if !counted {
		return 0, lastErr
	}
	return highestCount, nil
}

// isTransientCountError reports whether a failed point count can succeed when it is tried again.
func isTransientCountError(err error) bool {
	switch status.Code(err) {
	case codes.Internal, codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	}
	return false
}
