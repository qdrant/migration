package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

func init() {
	registerCommand[MigrateFromChromaCmd]("chroma", "Migrate data from a Chroma database to Qdrant.")
}

type MigrateFromChromaCmd struct {
	Chroma         commons.ChromaConfig    `embed:"" prefix:"chroma."`
	Qdrant         commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration      commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField        string                  `prefix:"qdrant." help:"Field storing Chroma IDs in Qdrant." default:"__id__"`
	DistanceMetric string                  `prefix:"qdrant." enum:",cosine,dot,euclid,manhattan" help:"Distance metric for the Qdrant collection. Defaults to the space of the Chroma collection." default:""`
	DocumentField  string                  `prefix:"qdrant." help:"Field storing Chroma documents in Qdrant." default:"document"`
	UriField       string                  `prefix:"qdrant." help:"Field storing Chroma URIs in Qdrant." default:"uri"`

	targetHost string
	targetPort int
	targetTLS  bool

	httpClient *http.Client
}

type chromaSpaceConfig struct {
	Space string `json:"space"`
}

type chromaCollection struct {
	ID            string         `json:"id"`
	Dimension     *uint64        `json:"dimension"`
	Metadata      map[string]any `json:"metadata"`
	Configuration struct {
		Hnsw  chromaSpaceConfig `json:"hnsw"`
		Spann chromaSpaceConfig `json:"spann"`
	} `json:"configuration_json"`
	Schema struct {
		Keys map[string]struct {
			SparseVector struct {
				SparseVectorIndex struct {
					Enabled bool `json:"enabled"`
					Config  struct {
						BM25 bool `json:"bm25"`
					} `json:"config"`
				} `json:"sparse_vector_index"`
			} `json:"sparse_vector"`
		} `json:"keys"`
	} `json:"schema"`
}

type chromaGetResponse struct {
	IDs        []string         `json:"ids"`
	Embeddings [][]float32      `json:"embeddings"`
	Documents  []*string        `json:"documents"`
	URIs       []*string        `json:"uris"`
	Metadatas  []map[string]any `json:"metadatas"`
}

func (r *MigrateFromChromaCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	chromaURL, err := url.Parse(r.Chroma.Url)
	if err != nil || (chromaURL.Scheme != "http" && chromaURL.Scheme != "https") || chromaURL.Host == "" {
		return fmt.Errorf("invalid Chroma URL %q, expected http://host:port or https://host:port", r.Chroma.Url)
	}
	r.Chroma.Url = strings.TrimRight(r.Chroma.Url, "/")

	return nil
}

func (r *MigrateFromChromaCmd) Validate() error {
	switch r.Chroma.AuthType {
	case "basic":
		if r.Chroma.Username == "" || r.Chroma.Password == "" {
			return errors.New("username and password are required for basic authentication")
		}
	case "token":
		if r.Chroma.Token == "" || r.Chroma.TokenHeader == "" {
			return errors.New("token and token header are required for token authentication")
		}
	}

	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromChromaCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Chroma to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: globals.SkipTlsVerification}
	transport.ResponseHeaderTimeout = 60 * time.Second
	r.httpClient = &http.Client{Transport: transport}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var collection chromaCollection
	err = r.request(ctx, http.MethodGet, r.collectionPath(r.Chroma.Collection), nil, &collection)
	if err != nil {
		return fmt.Errorf("failed to get Chroma collection: %w", err)
	}

	if collection.Dimension == nil {
		pterm.Info.Printfln("Chroma collection '%s' has no records, nothing to migrate.", r.Chroma.Collection)
		return nil
	}

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	var sourcePointCount uint64
	err = r.request(ctx, http.MethodGet, r.collectionPath(collection.ID)+"/count", nil, &sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, &collection, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("chroma", r.Chroma.Collection, r.Qdrant.Collection)

	err = r.migrateData(ctx, &collection, targetClient, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	err = verifyTargetCount(ctx, targetClient, r.Qdrant.Collection, sourcePointCount, r.Migration.OffsetsCollection)
	if err != nil {
		return err
	}

	err = commons.DeleteOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}

	return nil
}

func (r *MigrateFromChromaCmd) collectionPath(collection string) string {
	return fmt.Sprintf("/api/v2/tenants/%s/databases/%s/collections/%s",
		url.PathEscape(r.Chroma.Tenant), url.PathEscape(r.Chroma.Database), url.PathEscape(collection))
}

// request retries network errors, rate limits, and server errors with exponential backoff.
func (r *MigrateFromChromaCmd) request(ctx context.Context, method, path string, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	var err error
	for attempt := 0; attempt < maxUpsertRetries; attempt++ {
		var retryable bool
		retryable, err = r.requestOnce(ctx, method, path, payload, out)
		if err == nil || !retryable || ctx.Err() != nil {
			return err
		}
		time.Sleep(baseRetryDelay * time.Duration(1<<attempt))
	}
	return err
}

func (r *MigrateFromChromaCmd) requestOnce(ctx context.Context, method, path string, payload []byte, out any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, method, r.Chroma.Url+path, bytes.NewReader(payload))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	switch r.Chroma.AuthType {
	case "basic":
		req.SetBasicAuth(r.Chroma.Username, r.Chroma.Password)
	case "token":
		if strings.EqualFold(r.Chroma.TokenHeader, "Authorization") {
			req.Header.Set("Authorization", "Bearer "+r.Chroma.Token)
		} else {
			req.Header.Set(r.Chroma.TokenHeader, r.Chroma.Token)
		}
	}

	res, err := r.httpClient.Do(req)
	if err != nil {
		return true, fmt.Errorf("request to Chroma failed: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(res.Body)
		retryable := res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= http.StatusInternalServerError
		return retryable, fmt.Errorf("request %s %s failed (status %d): %s", method, path, res.StatusCode, respBody)
	}

	// UseNumber keeps integer metadata apart from floats.
	decoder := json.NewDecoder(res.Body)
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return false, fmt.Errorf("failed to decode Chroma response: %w", err)
	}

	return false, nil
}

// distance maps the Chroma space to a Qdrant distance. The legacy "hnsw:space" metadata comes first,
// because collections created before Chroma 1.0 can have the default space in their configuration.
// Chroma's "l2" and "ip" order results like Qdrant's Euclid and Dot.
func (c *chromaCollection) distance() (qdrant.Distance, error) {
	legacySpace, _ := c.Metadata["hnsw:space"].(string)
	space := "l2"
	for _, candidate := range []string{legacySpace, c.Configuration.Hnsw.Space, c.Configuration.Spann.Space} {
		if candidate != "" {
			space = candidate
			break
		}
	}

	switch space {
	case "l2":
		return qdrant.Distance_Euclid, nil
	case "cosine":
		return qdrant.Distance_Cosine, nil
	case "ip":
		return qdrant.Distance_Dot, nil
	default:
		return qdrant.Distance_UnknownDistance, fmt.Errorf("unsupported Chroma space %q", space)
	}
}

// sparseVectors returns the metadata keys that have a Chroma sparse vector index.
// BM25 sparse vectors get the IDF modifier, because Chroma applies IDF to them at query time.
func (c *chromaCollection) sparseVectors() map[string]*qdrant.SparseVectorParams {
	sparseVectors := make(map[string]*qdrant.SparseVectorParams)
	for key, valueTypes := range c.Schema.Keys {
		index := valueTypes.SparseVector.SparseVectorIndex
		if !index.Enabled {
			continue
		}
		params := &qdrant.SparseVectorParams{}
		if index.Config.BM25 {
			params.Modifier = qdrant.Modifier_Idf.Enum()
		}
		sparseVectors[key] = params
	}
	return sparseVectors
}

func (r *MigrateFromChromaCmd) prepareTargetCollection(ctx context.Context, collection *chromaCollection, targetClient *qdrant.Client) error {
	if !r.Migration.CreateCollection {
		return nil
	}

	targetCollectionExists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}

	if targetCollectionExists {
		pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}

	distanceMapping := map[string]qdrant.Distance{
		"euclid":    qdrant.Distance_Euclid,
		"cosine":    qdrant.Distance_Cosine,
		"dot":       qdrant.Distance_Dot,
		"manhattan": qdrant.Distance_Manhattan,
	}

	distance, ok := distanceMapping[r.DistanceMetric]
	if !ok {
		distance, err = collection.distance()
		if err != nil {
			return err
		}
	}

	createReq := &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     *collection.Dimension,
			Distance: distance,
		}),
	}
	if sparseVectors := collection.sparseVectors(); len(sparseVectors) > 0 {
		createReq.SparseVectorsConfig = qdrant.NewSparseVectorsConfig(sparseVectors)
	}

	if err := targetClient.CreateCollection(ctx, createReq); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection '%s'", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromChromaCmd) migrateData(ctx context.Context, collection *chromaCollection, targetClient *qdrant.Client, sourcePointCount uint64) error {
	batchSize := r.Migration.BatchSize
	sparseVectors := collection.sparseVectors()

	var currentOffset uint64 = 0

	if !r.Migration.Restart {
		_, offsetStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, collection.ID)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		currentOffset = offsetStored
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, currentOffset)

	skippedSparseVectors := false
	for {
		var resp chromaGetResponse
		err := r.request(ctx, http.MethodPost, r.collectionPath(collection.ID)+"/get", map[string]any{
			"limit":   batchSize,
			"offset":  currentOffset,
			"include": []string{"documents", "embeddings", "metadatas", "uris"},
		}, &resp)
		if err != nil {
			return fmt.Errorf("failed to get vectors from Chroma: %w", err)
		}

		count := len(resp.IDs)
		if count == 0 {
			break
		}
		if len(resp.Embeddings) != count || len(resp.Documents) != count || len(resp.URIs) != count || len(resp.Metadatas) != count {
			return fmt.Errorf("the Chroma response has %d IDs, %d embeddings, %d documents, %d URIs and %d metadatas",
				count, len(resp.Embeddings), len(resp.Documents), len(resp.URIs), len(resp.Metadatas))
		}

		targetPoints := make([]*qdrant.PointStruct, 0, count)
		for i, id := range resp.IDs {
			payload := make(map[string]any)
			vectors := map[string]*qdrant.Vector{"": qdrant.NewVectorDense(resp.Embeddings[i])}
			for key, value := range resp.Metadatas[i] {
				// Objects in Chroma metadata are sparse vectors.
				if _, ok := value.(map[string]any); ok {
					if _, indexed := sparseVectors[key]; indexed {
						vector, err := chromaSparseVector(value)
						if err != nil {
							return fmt.Errorf("invalid sparse vector %q of record %q: %w", key, id, err)
						}
						vectors[key] = vector
					} else if !skippedSparseVectors {
						pterm.Warning.Printfln("Skipping sparse vectors without a sparse vector index, such as '%s'", key)
						skippedSparseVectors = true
					}
					continue
				}
				if key == r.IdField || key == r.DocumentField || key == r.UriField {
					return fmt.Errorf("metadata key %q of record %q has the name of an ID, document or URI field, set --qdrant.id-field, --qdrant.document-field or --qdrant.uri-field to another name", key, id)
				}
				payload[key] = convertChromaValue(value)
			}
			payload[r.IdField] = id
			if resp.Documents[i] != nil {
				payload[r.DocumentField] = *resp.Documents[i]
			}
			if resp.URIs[i] != nil {
				payload[r.UriField] = *resp.URIs[i]
			}

			qdrantPayload, err := qdrant.TryValueMap(payload)
			if err != nil {
				return fmt.Errorf("failed to convert metadata of record %q: %w", id, err)
			}

			targetPoints = append(targetPoints, &qdrant.PointStruct{
				Id:      arbitraryIDToUUID(id),
				Vectors: qdrant.NewVectorsMap(vectors),
				Payload: qdrantPayload,
			})
		}

		err = upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return err
		}

		currentOffset += uint64(count)
		// Just a placeholder ID for offset tracking.
		// We're only using the offset count
		offsetId := qdrant.NewIDNum(0)
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, collection.ID, offsetId, currentOffset)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(count)

		// Apply batch delay if configured (helps with rate limiting)
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// convertChromaValue turns integer JSON numbers into int64 and other numbers into float64.
func convertChromaValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		f, _ := v.Float64()
		return f
	case []any:
		for i, item := range v {
			v[i] = convertChromaValue(item)
		}
	}
	return value
}

func chromaSparseVector(value any) (*qdrant.Vector, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var sparse struct {
		Indices []uint32  `json:"indices"`
		Values  []float32 `json:"values"`
	}
	if err := json.Unmarshal(data, &sparse); err != nil {
		return nil, err
	}
	return qdrant.NewVectorSparse(sparse.Indices, sparse.Values), nil
}
