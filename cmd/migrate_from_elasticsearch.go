package cmd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromElasticsearchCmd struct {
	Elasticsearch commons.ElasticsearchConfig `embed:"" prefix:"elasticsearch."`
	Qdrant        commons.QdrantConfig        `embed:"" prefix:"qdrant."`
	Migration     commons.MigrationConfig     `embed:"" prefix:"migration."`
	IdField       string                      `prefix:"qdrant." help:"Field storing Elasticsearch IDs in Qdrant." default:"__id__"`

	targetHost   string
	targetPort   int
	targetTLS    bool
	vectorFields []string
}

func (r *MigrateFromElasticsearchCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromElasticsearchCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromElasticsearchCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Elasticsearch to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := r.connectToElasticsearch()
	if err != nil {
		return fmt.Errorf("failed to connect to Elasticsearch source: %w", err)
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

	sourcePointCount, err := r.countElasticsearchDocuments(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to count documents in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceClient, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("elasticsearch", r.Elasticsearch.Index, r.Qdrant.Collection)

	err = r.migrateData(ctx, sourceClient, targetClient, sourcePointCount)
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

	return nil
}

func (r *MigrateFromElasticsearchCmd) connectToElasticsearch() (*elasticsearch.Client, error) {
	config := elasticsearch.Config{
		Addresses: []string{r.Elasticsearch.Url},
		Username:  r.Elasticsearch.Username,
		Password:  r.Elasticsearch.Password,
		APIKey:    r.Elasticsearch.APIKey,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: r.Elasticsearch.InsecureSkipVerify,
			},
		},
	}

	client, err := elasticsearch.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	return client, nil
}

func (r *MigrateFromElasticsearchCmd) countElasticsearchDocuments(ctx context.Context, client *elasticsearch.Client) (int64, error) {
	req := esapi.CountRequest{
		Index: []string{r.Elasticsearch.Index},
	}
	res, err := req.Do(ctx, client)
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}
	defer res.Body.Close()

	var result map[string]any
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode count response: %w", err)
	}

	count, ok := result["count"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid count response format")
	}

	return int64(count), nil
}

func (r *MigrateFromElasticsearchCmd) prepareTargetCollection(ctx context.Context, sourceClient *elasticsearch.Client, targetClient *qdrant.Client) error {
	mappingRes, err := esapi.IndicesGetMappingRequest{
		Index: []string{r.Elasticsearch.Index},
	}.Do(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to get index mapping: %w", err)
	}
	defer mappingRes.Body.Close()

	var mapping map[string]any
	if err := json.NewDecoder(mappingRes.Body).Decode(&mapping); err != nil {
		return fmt.Errorf("failed to decode mapping response: %w", err)
	}

	vectorParamsMap, err := r.extractVectorFields(mapping)
	if err != nil {
		return fmt.Errorf("failed to extract vector fields: %w", err)
	}

	if !r.Migration.CreateCollection {
		return nil
	}

	targetCollectionExists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}

	if targetCollectionExists {
		pterm.Info.Printfln("Target collection %q already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}

	err = targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig:  qdrant.NewVectorsConfigMap(vectorParamsMap),
	})
	if err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	pterm.Success.Printfln("Created target collection %q", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromElasticsearchCmd) extractVectorFields(mapping map[string]any) (map[string]*qdrant.VectorParams, error) {
	vectorParamsMap := make(map[string]*qdrant.VectorParams)
	var vectorFieldNames []string

	indexMapping, ok := mapping[r.Elasticsearch.Index].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid mapping structure: missing index")
	}

	mappings, ok := indexMapping["mappings"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid mapping structure: missing mappings")
	}

	properties, ok := mappings["properties"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid mapping structure: missing properties")
	}

	distanceMapping := map[string]qdrant.Distance{
		"l2_norm":           qdrant.Distance_Euclid,
		"cosine":            qdrant.Distance_Cosine,
		"dot_product":       qdrant.Distance_Dot,
		"max_inner_product": qdrant.Distance_Dot,
	}

	for fieldName, fieldDef := range properties {
		fieldProps, ok := fieldDef.(map[string]any)
		if !ok {
			continue
		}

		fieldType, ok := fieldProps["type"].(string)
		if !ok || fieldType != "dense_vector" {
			continue
		}

		dimValue, ok := fieldProps["dims"]
		if !ok {
			continue
		}

		dimension, ok := dimValue.(float64)
		if !ok {
			return nil, fmt.Errorf("invalid dimension type for field %s: expected int, got %T", fieldName, dimValue)
		}

		var distance qdrant.Distance
		if similarity, ok := fieldProps["similarity"].(string); ok {
			if mappedDistance, exists := distanceMapping[strings.ToLower(similarity)]; exists {
				distance = mappedDistance
			}
		}

		if distance == qdrant.Distance_UnknownDistance {
			distance = qdrant.Distance_Cosine
			pterm.Warning.Printfln("Unsupported similarity for field '%s', defaulting to cosine distance", fieldName)
		}

		vectorParamsMap[fieldName] = &qdrant.VectorParams{
			Size:     uint64(dimension),
			Distance: distance,
		}
		vectorFieldNames = append(vectorFieldNames, fieldName)
	}

	r.vectorFields = vectorFieldNames

	return vectorParamsMap, nil
}

func (r *MigrateFromElasticsearchCmd) migrateData(ctx context.Context, sourceClient *elasticsearch.Client, targetClient *qdrant.Client, sourcePointCount int64) error {
	batchSize := r.Migration.BatchSize

	offsetCount := uint64(0)
	var lastSortValue any

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Elasticsearch.Index)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
		if id != nil {
			if id.GetUuid() != "" {
				lastSortValue = id.GetUuid()
			} else if id.GetNum() != 0 {
				lastSortValue = id.GetNum()
			}
		}
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		hits, err := r.searchWithPagination(ctx, sourceClient, batchSize, lastSortValue)
		if err != nil {
			return fmt.Errorf("failed to search documents: %w", err)
		}

		if len(hits) == 0 {
			break
		}

		var targetPoints []*qdrant.PointStruct
		for _, hit := range hits {
			doc, ok := hit.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid hit format: expected map[string]any, got %T", hit)
			}
			source, ok := doc["_source"].(map[string]any)
			if !ok {
				return fmt.Errorf("invalid _source format: expected map[string]any, got %T", doc["_source"])
			}
			docID, ok := doc["_id"].(string)
			if !ok {
				return fmt.Errorf("invalid _id format: expected string, got %T", doc["_id"])
			}

			point := &qdrant.PointStruct{}
			vectors := make(map[string]*qdrant.Vector)
			payload := make(map[string]any)

			point.Id = arbitraryIDToUUID(docID)
			payload[r.IdField] = docID

			// Process regular "_source" fields
			for fieldName, value := range source {
				if vector, ok := extractElasticsearchVector(value); ok {
					vectors[fieldName] = qdrant.NewVector(vector...)
				} else {
					payload[fieldName] = value
				}
			}

			// Look for vectors within "fields"
			// Ref: https://www.elastic.co/search-labs/blog/elasticsearch-exclude-vectors-from-source
			if fieldsData, ok := doc["fields"].(map[string]any); ok {
				for fieldName, value := range fieldsData {
					if vector, ok := extractElasticsearchVector(value); ok {
						vectors[fieldName] = qdrant.NewVector(vector...)
					}
				}
			}

			point.Vectors = qdrant.NewVectorsMap(vectors)

			point.Payload = qdrant.NewValueMap(payload)
			targetPoints = append(targetPoints, point)
		}

		_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return fmt.Errorf("failed to insert data into target: %w", err)
		}

		offsetCount += uint64(len(targetPoints))

		lastDoc, ok := hits[len(hits)-1].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid last hit format: expected map[string]any, got %T", hits[len(hits)-1])
		}

		sortValues, ok := lastDoc["sort"].([]any)
		if !ok || len(sortValues) == 0 {
			return fmt.Errorf("sort values not found in response - this should not happen with _doc sorting")
		}
		lastSortValue = sortValues[0]

		var offsetID *qdrant.PointId
		switch v := lastSortValue.(type) {
		case float64:
			offsetID = qdrant.NewIDNum(uint64(v))
		case int64:
			offsetID = qdrant.NewIDNum(uint64(v))
		default:
			offsetID = qdrant.NewID(fmt.Sprintf("%v", v))
		}

		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Elasticsearch.Index, offsetID, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

func (r *MigrateFromElasticsearchCmd) searchWithPagination(ctx context.Context, client *elasticsearch.Client, size int, searchAfter any) ([]any, error) {
	searchRequest := map[string]any{
		"size": size,
		"sort": []string{"_doc"},
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	}

	if len(r.vectorFields) > 0 {
		searchRequest["fields"] = r.vectorFields
	}

	if searchAfter != nil {
		searchRequest["search_after"] = []any{searchAfter}
	}

	requestBody, err := json.Marshal(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search request: %w", err)
	}

	res, err := esapi.SearchRequest{
		Index: []string{r.Elasticsearch.Index},
		Body:  strings.NewReader(string(requestBody)),
	}.Do(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("search request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var errorResp map[string]any
		if err := json.NewDecoder(res.Body).Decode(&errorResp); err != nil {
			return nil, fmt.Errorf("elasticsearch request failed with status %d", res.StatusCode)
		}
		return nil, fmt.Errorf("elasticsearch error: %v", errorResp)
	}

	var searchResp map[string]any
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	hitsContainer, ok := searchResp["hits"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid response format: missing hits container")
	}

	hits, ok := hitsContainer["hits"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid response format: missing hits array")
	}

	return hits, nil
}

func extractElasticsearchVector(value any) ([]float32, bool) {
	switch v := value.(type) {
	case []any:
		if len(v) == 0 {
			return nil, false
		}
		vector := make([]float32, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float32:
				vector[i] = n
			case float64:
				vector[i] = float32(n)
			case int:
				vector[i] = float32(n)
			case int32:
				vector[i] = float32(n)
			case int64:
				vector[i] = float32(n)
			default:
				return nil, false
			}
		}
		return vector, true
	case []float32:
		return v, true
	case []float64:
		vector := make([]float32, len(v))
		for i, val := range v {
			vector[i] = float32(val)
		}
		return vector, true
	default:
		return nil, false
	}
}
