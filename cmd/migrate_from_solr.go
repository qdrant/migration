package cmd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromSolrCmd struct {
	Solr      commons.SolrConfig      `embed:"" prefix:"solr."`
	Qdrant    commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField   string                  `prefix:"qdrant." help:"Field storing Solr IDs in Qdrant." default:"__id__"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromSolrCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromSolrCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromSolrCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Solr to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpClient := r.createHTTPClient()

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countSolrDocuments(ctx, httpClient)
	if err != nil {
		return fmt.Errorf("failed to count documents in source: %w", err)
	}

	displayMigrationStart("solr", r.Solr.Collection, r.Qdrant.Collection)

	err = r.migrateData(ctx, httpClient, targetClient, sourcePointCount)
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

func (r *MigrateFromSolrCmd) createHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: r.Solr.InsecureSkipVerify,
			},
		},
		Timeout: 30 * time.Second,
	}
}

func (r *MigrateFromSolrCmd) countSolrDocuments(ctx context.Context, client *http.Client) (int64, error) {
	solrURL := fmt.Sprintf("%s/solr/%s/select", r.Solr.Url, r.Solr.Collection)

	params := url.Values{}
	params.Set("q", "*:*")
	params.Set("rows", "0")
	params.Set("wt", "json")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, solrURL+"?"+params.Encode(), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	if r.Solr.Username != "" && r.Solr.Password != "" {
		req.SetBasicAuth(r.Solr.Username, r.Solr.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("solr request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	response, ok := result["response"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("invalid response format: missing 'response' field")
	}

	numFound, ok := response["numFound"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid response format: missing 'numFound' field")
	}

	return int64(numFound), nil
}

func (r *MigrateFromSolrCmd) migrateData(ctx context.Context, httpClient *http.Client, targetClient *qdrant.Client, sourcePointCount int64) error {
	batchSize := r.Migration.BatchSize

	offsetCount := uint64(0)
	cursorMark := "*"

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Solr.Collection)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
		if id != nil && id.GetUuid() != "" {
			cursorMark = id.GetUuid()
		}
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		docs, nextCursorMark, err := r.searchWithCursor(ctx, httpClient, batchSize, cursorMark)
		if err != nil {
			return fmt.Errorf("failed to search documents: %w", err)
		}

		if len(docs) == 0 {
			break
		}

		var targetPoints []*qdrant.PointStruct
		for _, doc := range docs {
			docMap, ok := doc.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid document format: expected map[string]any, got %T", doc)
			}

			var docID string
			if id, exists := docMap["id"]; exists {
				docID = fmt.Sprintf("%v", id)
			} else {
				docID = fmt.Sprintf("doc_%d", offsetCount)
			}

			point := &qdrant.PointStruct{}
			vectors := make(map[string]*qdrant.Vector)
			payload := make(map[string]any)

			point.Id = arbitraryIDToUUID(docID)
			payload[r.IdField] = docID

			for fieldName, value := range docMap {
				if vector, ok := extractSolrVector(value); ok {
					vectors[fieldName] = qdrant.NewVector(vector...)
				} else {
					payload[fieldName] = value
				}
			}

			point.Vectors = qdrant.NewVectorsMap(vectors)

			point.Payload = qdrant.NewValueMap(payload)
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

		offsetID := qdrant.NewID(nextCursorMark)
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Solr.Collection, offsetID, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))

		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}

		if nextCursorMark == cursorMark {
			break
		}
		cursorMark = nextCursorMark
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

func (r *MigrateFromSolrCmd) searchWithCursor(ctx context.Context, client *http.Client, rows int, cursorMark string) ([]any, string, error) {
	solrURL := fmt.Sprintf("%s/solr/%s/select", r.Solr.Url, r.Solr.Collection)

	params := url.Values{}
	params.Set("q", "*:*")
	params.Set("rows", fmt.Sprintf("%d", rows))
	params.Set("wt", "json")
	params.Set("sort", "id asc")
	params.Set("cursorMark", cursorMark)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, solrURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create request: %w", err)
	}

	if r.Solr.Username != "" && r.Solr.Password != "" {
		req.SetBasicAuth(r.Solr.Username, r.Solr.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("solr request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, "", fmt.Errorf("failed to decode response: %w", err)
	}

	response, ok := result["response"].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("invalid response format: missing 'response' field")
	}

	docs, ok := response["docs"].([]any)
	if !ok {
		return nil, "", fmt.Errorf("invalid response format: missing 'docs' array")
	}

	nextCursorMark, ok := result["nextCursorMark"].(string)
	if !ok {

		nextCursorMark = cursorMark
	}

	return docs, nextCursorMark, nil
}

func extractSolrVector(value any) ([]float32, bool) {
	switch v := value.(type) {
	case []any:
		vector := make([]float32, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float32:
				vector[i] = n
			case float64:
				vector[i] = float32(n)
			case int:
				vector[i] = float32(n)
			case int64:
				vector[i] = float32(n)
			default:
				return nil, false
			}
		}
		return vector, true
	default:
		return nil, false
	}
}
