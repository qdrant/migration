package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type azureVectorField struct {
	dimensions uint64
	distance   qdrant.Distance
}

type MigrateFromAzureCmd struct {
	Azure     commons.AzureSearchConfig `embed:"" prefix:"azure."`
	Qdrant    commons.QdrantConfig      `embed:"" prefix:"qdrant."`
	Migration commons.MigrationConfig   `embed:"" prefix:"migration."`
	IdField   string                    `prefix:"qdrant." help:"Field storing Azure AI Search document keys in Qdrant." default:"__id__"`

	targetHost string
	targetPort int
	targetTLS  bool

	httpClient   *http.Client
	keyField     string
	vectorFields map[string]azureVectorField
}

func (r *MigrateFromAzureCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	r.Azure.Endpoint = strings.TrimRight(r.Azure.Endpoint, "/")

	return nil
}

func (r *MigrateFromAzureCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromAzureCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Azure AI Search to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	r.httpClient = &http.Client{
		Timeout: 60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	err = r.loadIndexDefinition(ctx)
	if err != nil {
		return fmt.Errorf("failed to load index definition: %w", err)
	}

	sourcePointCount, err := r.countDocuments(ctx)
	if err != nil {
		return fmt.Errorf("failed to count documents in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("azure-ai-search", r.Azure.Index, r.Qdrant.Collection)

	err = r.migrateData(ctx, targetClient, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Qdrant.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	pterm.Info.Printfln("Target collection has %d points\n", targetPointCount)

	err = commons.DeleteOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}

	return nil
}

func (r *MigrateFromAzureCmd) loadIndexDefinition(ctx context.Context) error {
	indexDef, err := r.request(ctx, http.MethodGet, "/indexes/"+r.Azure.Index, nil)
	if err != nil {
		return err
	}

	metrics := extractAzureMetrics(indexDef)
	r.vectorFields = make(map[string]azureVectorField)

	fields, _ := indexDef["fields"].([]any)
	for _, fieldRaw := range fields {
		field, ok := fieldRaw.(map[string]any)
		if !ok {
			continue
		}

		name, _ := field["name"].(string)

		if isKey, _ := field["key"].(bool); isKey {
			sortable, _ := field["sortable"].(bool)
			filterable, _ := field["filterable"].(bool)
			if !sortable || !filterable {
				return fmt.Errorf("key field %q must be sortable and filterable for keyset pagination", name)
			}
			r.keyField = name
			continue
		}

		dimensions, ok := field["dimensions"].(float64)
		if !ok {
			continue
		}

		profile, _ := field["vectorSearchProfile"].(string)
		r.vectorFields[name] = azureVectorField{
			dimensions: uint64(dimensions),
			distance:   azureDistance(metrics[profile]),
		}
	}

	if r.keyField == "" {
		return fmt.Errorf("index %q does not declare a key field", r.Azure.Index)
	}

	return nil
}

func extractAzureMetrics(indexDef map[string]any) map[string]string {
	vectorSearch, _ := indexDef["vectorSearch"].(map[string]any)

	algorithms := make(map[string]string)
	if algs, ok := vectorSearch["algorithms"].([]any); ok {
		for _, algRaw := range algs {
			alg, ok := algRaw.(map[string]any)
			if !ok {
				continue
			}
			name, _ := alg["name"].(string)
			metric := ""
			if hnsw, ok := alg["hnswParameters"].(map[string]any); ok {
				metric, _ = hnsw["metric"].(string)
			} else if eknn, ok := alg["exhaustiveKnnParameters"].(map[string]any); ok {
				metric, _ = eknn["metric"].(string)
			}
			algorithms[name] = metric
		}
	}

	result := make(map[string]string)
	if profiles, ok := vectorSearch["profiles"].([]any); ok {
		for _, profileRaw := range profiles {
			profile, ok := profileRaw.(map[string]any)
			if !ok {
				continue
			}
			name, _ := profile["name"].(string)
			algorithm, _ := profile["algorithm"].(string)
			result[name] = algorithms[algorithm]
		}
	}

	return result
}

func azureDistance(metric string) qdrant.Distance {
	switch strings.ToLower(metric) {
	case "dotproduct":
		return qdrant.Distance_Dot
	case "euclidean":
		return qdrant.Distance_Euclid
	default:
		return qdrant.Distance_Cosine
	}
}

func (r *MigrateFromAzureCmd) countDocuments(ctx context.Context) (int64, error) {
	resp, err := r.request(ctx, http.MethodPost, "/indexes/"+r.Azure.Index+"/docs/search", map[string]any{
		"search": "*",
		"top":    0,
		"count":  true,
	})
	if err != nil {
		return 0, err
	}

	count, ok := resp["@odata.count"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid count response: missing '@odata.count'")
	}

	return int64(count), nil
}

func (r *MigrateFromAzureCmd) prepareTargetCollection(ctx context.Context, targetClient *qdrant.Client) error {
	if !r.Migration.CreateCollection {
		return nil
	}

	exists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if exists {
		pterm.Info.Printfln("Target collection %q already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}

	vectorParams := make(map[string]*qdrant.VectorParams, len(r.vectorFields))
	for name, field := range r.vectorFields {
		vectorParams[name] = &qdrant.VectorParams{
			Size:     field.dimensions,
			Distance: field.distance,
		}
	}

	err = targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig:  qdrant.NewVectorsConfigMap(vectorParams),
	})
	if err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection %q", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromAzureCmd) migrateData(ctx context.Context, targetClient *qdrant.Client, sourcePointCount int64) error {
	offsetCount := uint64(0)
	var lastKey string

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Azure.Index)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
		if id != nil && id.GetUuid() != "" {
			lastKey = id.GetUuid()
		}
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		docs, err := r.searchDocuments(ctx, r.Migration.BatchSize, lastKey)
		if err != nil {
			return fmt.Errorf("failed to search documents: %w", err)
		}
		if len(docs) == 0 {
			break
		}

		targetPoints := make([]*qdrant.PointStruct, 0, len(docs))
		for _, doc := range docs {
			point, err := r.convertDocument(doc)
			if err != nil {
				return err
			}
			targetPoints = append(targetPoints, point)
		}

		err = upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return err
		}

		offsetCount += uint64(len(targetPoints))
		lastDoc := docs[len(docs)-1]
		key, ok := lastDoc[r.keyField].(string)
		if !ok {
			return fmt.Errorf("invalid key value: expected string, got %T", lastDoc[r.keyField])
		}
		lastKey = key

		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Azure.Index, qdrant.NewIDUUID(lastKey), offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))

		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

func (r *MigrateFromAzureCmd) convertDocument(doc map[string]any) (*qdrant.PointStruct, error) {
	key, ok := doc[r.keyField].(string)
	if !ok {
		return nil, fmt.Errorf("invalid key value: expected string, got %T", doc[r.keyField])
	}

	vectors := make(map[string]*qdrant.Vector)
	payload := map[string]any{r.IdField: key}

	for name, value := range doc {
		if strings.HasPrefix(name, "@search.") {
			continue
		}

		if _, isVector := r.vectorFields[name]; isVector {
			if vector, ok := extractAzureVector(value); ok {
				vectors[name] = qdrant.NewVector(vector...)
			}
			continue
		}

		payload[name] = value
	}

	return &qdrant.PointStruct{
		Id:      arbitraryIDToUUID(key),
		Vectors: qdrant.NewVectorsMap(vectors),
		Payload: qdrant.NewValueMap(payload),
	}, nil
}

func (r *MigrateFromAzureCmd) searchDocuments(ctx context.Context, top int, lastKey string) ([]map[string]any, error) {
	body := map[string]any{
		"search":  "*",
		"top":     top,
		"select":  "*",
		"orderby": r.keyField,
	}
	if lastKey != "" {
		body["filter"] = fmt.Sprintf("%s gt '%s'", r.keyField, escapeODataString(lastKey))
	}

	resp, err := r.request(ctx, http.MethodPost, "/indexes/"+r.Azure.Index+"/docs/search", body)
	if err != nil {
		return nil, err
	}

	value, _ := resp["value"].([]any)
	docs := make([]map[string]any, 0, len(value))
	for _, raw := range value {
		doc, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid document format: expected object, got %T", raw)
		}
		docs = append(docs, doc)
	}

	return docs, nil
}

func (r *MigrateFromAzureCmd) request(ctx context.Context, method, path string, body any) (map[string]any, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, r.Azure.Endpoint+path+"?api-version="+r.Azure.APIVersion, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("api-key", r.Azure.APIKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to Azure AI Search failed: %w", err)
	}
	defer res.Body.Close()

	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("azure AI Search request failed (status %d): %s", res.StatusCode, string(respBody))
	}

	var resp map[string]any
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return resp, nil
}

func escapeODataString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func extractAzureVector(value any) ([]float32, bool) {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil, false
	}

	vector := make([]float32, len(items))
	for i, item := range items {
		n, ok := item.(float64)
		if !ok {
			return nil, false
		}
		vector[i] = float32(n)
	}

	return vector, true
}
