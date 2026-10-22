package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/qdrant/go-client/qdrant"
	"github.com/qdrant/go-client/qdrant/serverless"
)

func TestServerlessConfigFromCollectionInfo(t *testing.T) {
	info := &qdrant.CollectionInfo{
		Config: &qdrant.CollectionConfig{
			Params: &qdrant.CollectionParams{
				VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
					"image": {Size: 512, Distance: qdrant.Distance_Cosine},
					"colbert": {
						Size:              128,
						Distance:          qdrant.Distance_Dot,
						MultivectorConfig: &qdrant.MultiVectorConfig{Comparator: qdrant.MultiVectorComparator_MaxSim},
					},
				}),
				SparseVectorsConfig: qdrant.NewSparseVectorsConfig(map[string]*qdrant.SparseVectorParams{
					"bm25":   {Modifier: qdrant.Modifier_Idf.Enum()},
					"splade": {},
				}),
			},
		},
		PayloadSchema: map[string]*qdrant.PayloadSchemaInfo{
			"color": {DataType: qdrant.PayloadSchemaType_Keyword},
			"age": {
				DataType: qdrant.PayloadSchemaType_Integer,
				Params:   qdrant.NewPayloadIndexParamsInt(&qdrant.IntegerIndexParams{Range: qdrant.PtrOf(false)}),
			},
			"title": {
				DataType: qdrant.PayloadSchemaType_Text,
				Params: qdrant.NewPayloadIndexParamsText(&qdrant.TextIndexParams{
					Tokenizer: qdrant.TokenizerType_Word,
					Stopwords: &qdrant.StopwordsSet{Languages: []string{"english"}},
					Stemmer: &qdrant.StemmingAlgorithm{StemmingParams: &qdrant.StemmingAlgorithm_Snowball{
						Snowball: &qdrant.SnowballParams{Language: "english"},
					}},
				}),
			},
			"location": {DataType: qdrant.PayloadSchemaType_Geo},
			// A Qdrant cluster defaults to the word tokenizer, and Qdrant Serverless to the whitespace tokenizer.
			"body": {DataType: qdrant.PayloadSchemaType_Text},
		},
	}

	config, err := serverlessConfigFromCollectionInfo(info)
	require.NoError(t, err)

	expected := &serverless.CollectionConfig{
		DenseVectors: map[string]*serverless.DenseVectorConfig{
			"image":   {Size: 512, Distance: serverless.Distance_COSINE},
			"colbert": {Size: 128, Distance: serverless.Distance_DOT, Multivector: true},
		},
		SparseVectors: map[string]*serverless.SparseVectorConfig{
			"bm25":   {UseIdf: true},
			"splade": {},
		},
		PayloadIndexes: map[string]*serverless.PayloadIndexConfig{
			"color": serverless.NewPayloadIndexConfigKeyword(nil),
			"age":   serverless.NewPayloadIndexConfigInteger(&serverless.IntegerIndex{Range: qdrant.PtrOf(false)}),
			"title": serverless.NewPayloadIndexConfigText(&serverless.TextIndex{
				Tokenizer: serverless.Tokenizer_WORD.Enum(),
				Stopwords: &serverless.StopwordsSet{Languages: []string{"english"}},
				Stemmer:   serverless.NewStemmingAlgorithmSnowball("english"),
			}),
			"location": serverless.NewPayloadIndexConfigGeo(),
			"body":     serverless.NewPayloadIndexConfigText(&serverless.TextIndex{Tokenizer: serverless.Tokenizer_WORD.Enum()}),
		},
	}
	require.True(t, proto.Equal(expected, config), "got: %v", config)
}

func TestCreateCollectionFromServerless(t *testing.T) {
	tests := []struct {
		name     string
		config   *serverless.CollectionConfig
		expected *qdrant.CreateCollection
	}{
		{
			name: "unnamed dense vector and sparse vector",
			config: &serverless.CollectionConfig{
				DenseVectors: map[string]*serverless.DenseVectorConfig{
					"": {Size: 4, Distance: serverless.Distance_MANHATTAN, PrecisionTier: serverless.PrecisionTier_LOW.Enum()},
				},
				SparseVectors: map[string]*serverless.SparseVectorConfig{
					"bm25": {UseIdf: true},
				},
			},
			expected: &qdrant.CreateCollection{
				CollectionName: "target",
				VectorsConfig:  qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 4, Distance: qdrant.Distance_Manhattan}),
				SparseVectorsConfig: qdrant.NewSparseVectorsConfig(map[string]*qdrant.SparseVectorParams{
					"bm25": {Modifier: qdrant.Modifier_Idf.Enum()},
				}),
			},
		},
		{
			name: "named dense vectors",
			config: &serverless.CollectionConfig{
				DenseVectors: map[string]*serverless.DenseVectorConfig{
					"text":    {Size: 1536, Distance: serverless.Distance_COSINE},
					"colbert": {Size: 128, Distance: serverless.Distance_DOT, Multivector: true},
				},
			},
			expected: &qdrant.CreateCollection{
				CollectionName: "target",
				VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
					"text": {Size: 1536, Distance: qdrant.Distance_Cosine},
					"colbert": {
						Size:              128,
						Distance:          qdrant.Distance_Dot,
						MultivectorConfig: &qdrant.MultiVectorConfig{Comparator: qdrant.MultiVectorComparator_MaxSim},
					},
				}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := createCollectionFromServerless("target", tt.config)
			require.NoError(t, err)
			require.True(t, proto.Equal(tt.expected, request), "got: %v", request)
		})
	}
}

// Converting the payload indexes to a Qdrant cluster schema and back must keep them unchanged.
func TestPayloadIndexesRoundTrip(t *testing.T) {
	indexes := map[string]*serverless.PayloadIndexConfig{
		"color":   serverless.NewPayloadIndexConfigKeyword(&serverless.KeywordIndex{Prefix: &serverless.KeywordPrefixParams{}}),
		"age":     serverless.NewPayloadIndexConfigInteger(&serverless.IntegerIndex{Lookup: qdrant.PtrOf(true), Range: qdrant.PtrOf(false)}),
		"price":   serverless.NewPayloadIndexConfigFloat(),
		"user_id": serverless.NewPayloadIndexConfigUUID(),
		"created": serverless.NewPayloadIndexConfigDatetime(),
		"geo":     serverless.NewPayloadIndexConfigGeo(),
		"active":  serverless.NewPayloadIndexConfigBool(),
		"title": serverless.NewPayloadIndexConfigText(&serverless.TextIndex{
			Tokenizer:    serverless.Tokenizer_MULTILINGUAL.Enum(),
			Lowercase:    qdrant.PtrOf(false),
			MinTokenLen:  qdrant.PtrOf(uint64(2)),
			AsciiFolding: qdrant.PtrOf(true),
			Stopwords:    &serverless.StopwordsSet{Languages: []string{"english"}, Custom: []string{"foo"}},
			Stemmer:      serverless.NewStemmingAlgorithmSnowball("english"),
		}),
		"body": serverless.NewPayloadIndexConfigText(&serverless.TextIndex{
			Tokenizer: serverless.Tokenizer_WHITESPACE.Enum(),
			Stemmer:   serverless.NewStemmingAlgorithmDisabled(),
		}),
	}

	schema := payloadSchemaFromServerless(indexes)
	require.Len(t, schema, len(indexes))

	for field, index := range indexes {
		got := serverlessPayloadIndex(schema[field])
		require.True(t, proto.Equal(index, got), "field %q: got %v, expected %v", field, got, index)
	}
}

func TestServerlessVectorsConfig(t *testing.T) {
	tests := []struct {
		name     string
		request  *qdrant.CreateCollection
		expected *serverless.CollectionConfig
		err      string
	}{
		{
			name: "unnamed dense vector",
			request: &qdrant.CreateCollection{
				VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 768, Distance: qdrant.Distance_Euclid}),
			},
			expected: &serverless.CollectionConfig{
				DenseVectors: map[string]*serverless.DenseVectorConfig{
					"": {Size: 768, Distance: serverless.Distance_EUCLID},
				},
			},
		},
		{
			name: "sparse vector only",
			request: &qdrant.CreateCollection{
				SparseVectorsConfig: qdrant.NewSparseVectorsConfig(map[string]*qdrant.SparseVectorParams{
					"sparse_vector": {},
				}),
			},
			expected: &serverless.CollectionConfig{
				SparseVectors: map[string]*serverless.SparseVectorConfig{
					"sparse_vector": {},
				},
			},
		},
		{
			name: "unknown distance",
			request: &qdrant.CreateCollection{
				VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: 4}),
			},
			err: "does not support",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := serverlessVectorsConfig(tt.request.GetVectorsConfig(), tt.request.GetSparseVectorsConfig())
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.True(t, proto.Equal(tt.expected, config), "got: %v", config)
		})
	}
}

func TestValidateServerlessVectorNames(t *testing.T) {
	tests := []struct {
		name   string
		config *serverless.CollectionConfig
		err    string
	}{
		{
			name: "allowed characters",
			config: &serverless.CollectionConfig{
				DenseVectors:  map[string]*serverless.DenseVectorConfig{"": {}, "Image_2": {}},
				SparseVectors: map[string]*serverless.SparseVectorConfig{"a-b.c/d&e:f@g!h$i'j(k)l,m;n=o": {}},
			},
		},
		{
			name:   "space in a dense vector",
			config: &serverless.CollectionConfig{DenseVectors: map[string]*serverless.DenseVectorConfig{"ok": {}, "with space": {}}},
			err:    `vector name "with space" contains ' '`,
		},
		{
			name:   "non-ASCII letter in a sparse vector",
			config: &serverless.CollectionConfig{SparseVectors: map[string]*serverless.SparseVectorConfig{"ünï": {}}},
			err:    `vector name "ünï" contains 'ü'`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServerlessVectorNames(tt.config)
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestRetryServerlessPointRequests(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "no healthy upstream")
	reset := status.Error(codes.Internal, "stream terminated by RST_STREAM with error code: INTERNAL_ERROR")
	tests := []struct {
		name   string
		method string
		errs   []error
		calls  int
		err    error
	}{
		{name: "point request after temporary errors", method: "/qdrant.Points/Scroll", errs: []error{unavailable, reset, nil}, calls: 3},
		{name: "point request with an internal error", method: "/qdrant.Points/Count", errs: []error{status.Error(codes.Internal, "shard open task panicked")}, calls: 1, err: status.Error(codes.Internal, "")},
		{name: "point request with another error", method: "/qdrant.Points/Upsert", errs: []error{status.Error(codes.InvalidArgument, "bad")}, calls: 1, err: status.Error(codes.InvalidArgument, "bad")},
		{name: "collection request", method: "/qdrant.serverless.CollectionsService/DeleteCollection", errs: []error{unavailable}, calls: 1, err: unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			invoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				err := tt.errs[min(calls, len(tt.errs)-1)]
				calls++
				return err
			}
			err := retryServerlessPointRequests()(t.Context(), tt.method, nil, nil, nil, invoker)
			require.Equal(t, tt.calls, calls)
			if tt.err == nil {
				require.NoError(t, err)
				return
			}
			require.Equal(t, status.Code(tt.err), status.Code(err))
		})
	}
}

// Every command must be able to write to Qdrant Serverless.
func TestServerlessFlagOnAllCommands(t *testing.T) {
	parser, err := kong.New(&CLI{}, commandOptions()...)
	require.NoError(t, err)

	checked := 0
	for _, command := range parser.Model.Children {
		checked++
		// The placeholders for sources that a build leaves out take no flags.
		if _, unavailable := command.Target.Addr().Interface().(*unavailableCmd); command.Name == "completion" || unavailable {
			continue
		}
		flags := map[string]bool{}
		for _, flag := range command.Flags {
			flags[flag.Name] = true
		}
		require.True(t, flags["qdrant.serverless"] || flags["target.serverless"], "command %q has no serverless flag", command.Name)
	}
	// The commands and the completion command.
	require.Equal(t, len(commands)+1, checked)
}

type countResult struct {
	count uint64
	err   error
}

// fakeCounter returns the results in order and repeats the last one.
type fakeCounter struct {
	results []countResult
	calls   int
}

func (f *fakeCounter) Count(_ context.Context, _ *qdrant.CountPoints) (uint64, error) {
	result := f.results[min(f.calls, len(f.results)-1)]
	f.calls++
	return result.count, result.err
}

func shortenPointCountWait(t *testing.T) {
	t.Helper()
	countInterval := pointCountInterval
	pointCountInterval = time.Millisecond
	t.Cleanup(func() {
		pointCountInterval = countInterval
	})
}

func TestWaitForServerlessPointCount(t *testing.T) {
	shortenPointCountWait(t)
	transient := status.Error(codes.Internal, "shard open task panicked")

	t.Run("retries transient errors until the points show", func(t *testing.T) {
		counter := &fakeCounter{results: []countResult{{0, transient}, {0, nil}, {4, nil}, {0, transient}, {10, nil}}}
		count, err := waitForPointCount(t.Context(), counter, "c", 10, 50*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, uint64(10), count)
		require.Equal(t, 5, counter.calls)
	})

	t.Run("returns the highest count when the count stops growing", func(t *testing.T) {
		counter := &fakeCounter{results: []countResult{{3, nil}, {7, nil}, {5, nil}}}
		count, err := waitForPointCount(t.Context(), counter, "c", 10, 50*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, uint64(7), count)
	})

	t.Run("returns the transient error when no count succeeds", func(t *testing.T) {
		counter := &fakeCounter{results: []countResult{{0, transient}}}
		_, err := waitForPointCount(t.Context(), counter, "c", 10, 50*time.Millisecond)
		require.ErrorIs(t, err, transient)
	})

	t.Run("returns other errors at once", func(t *testing.T) {
		notFound := status.Error(codes.NotFound, "collection not found")
		counter := &fakeCounter{results: []countResult{{0, notFound}}}
		_, err := waitForPointCount(t.Context(), counter, "c", 10, 50*time.Millisecond)
		require.ErrorIs(t, err, notFound)
		require.Equal(t, 1, counter.calls)
	})

	t.Run("an empty collection with nothing expected", func(t *testing.T) {
		counter := &fakeCounter{results: []countResult{{0, nil}}}
		count, err := waitForPointCount(t.Context(), counter, "c", 0, 50*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, uint64(0), count)
		require.Equal(t, 1, counter.calls)
	})

	t.Run("stops when the context ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		counter := &fakeCounter{results: []countResult{{1, nil}}}
		_, err := waitForPointCount(ctx, counter, "c", 10, 50*time.Millisecond)
		require.ErrorIs(t, err, context.Canceled)
	})
}
