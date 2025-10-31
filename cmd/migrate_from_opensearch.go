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

	"github.com/opensearch-project/opensearch-go"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromOpenSearchCmd struct {
	OpenSearch commons.OpenSearchConfig `embed:"" prefix:"opensearch."`
	Qdrant     commons.QdrantConfig     `embed:"" prefix:"qdrant."`
	Migration  commons.MigrationConfig  `embed:"" prefix:"migration."`
	IdField    string                   `prefix:"qdrant." help:"Field storing OpenSearch IDs in Qdrant." default:"__id__"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromOpenSearchCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromOpenSearchCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromOpenSearchCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("OpenSearch to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := r.connectToOpenSearch()
	if err != nil {
		return fmt.Errorf("failed to connect to OpenSearch source: %w", err)
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

	sourcePointCount, err := r.countOpenSearchDocuments(ctx, sourceClient)
	if err != nil {
		return fmt.Errorf("failed to count documents in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceClient, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("opensearch", r.OpenSearch.Index, r.Qdrant.Collection)

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

func (r *MigrateFromOpenSearchCmd) connectToOpenSearch() (*opensearch.Client, error) {
	config := opensearch.Config{
		Addresses: []string{r.OpenSearch.Url},
		Username:  r.OpenSearch.Username,
		Password:  r.OpenSearch.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: r.OpenSearch.InsecureSkipVerify,
			},
		},
	}

	client, err := opensearch.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenSearch client: %w", err)
	}

	return client, nil
}

func (r *MigrateFromOpenSearchCmd) countOpenSearchDocuments(ctx context.Context, client *opensearch.Client) (int64, error) {
	res, err := client.Count(
		client.Count.WithIndex(r.OpenSearch.Index),
		client.Count.WithContext(ctx),
	)
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

func (r *MigrateFromOpenSearchCmd) prepareTargetCollection(ctx context.Context, sourceClient *opensearch.Client, targetClient *qdrant.Client) error {
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

	mappingRes, err := sourceClient.Indices.GetMapping(
		sourceClient.Indices.GetMapping.WithIndex(r.OpenSearch.Index),
	)
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

func (r *MigrateFromOpenSearchCmd) extractVectorFields(mapping map[string]any) (map[string]*qdrant.VectorParams, error) {
	vectorParamsMap := make(map[string]*qdrant.VectorParams)

	indexMapping, ok := mapping[r.OpenSearch.Index].(map[string]any)
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
		"l2":           qdrant.Distance_Euclid,
		"cosinesimil":  qdrant.Distance_Cosine,
		"innerproduct": qdrant.Distance_Dot,
		"l1":           qdrant.Distance_Manhattan,
	}

	for fieldName, fieldDef := range properties {
		fieldProps, ok := fieldDef.(map[string]any)
		if !ok {
			continue
		}

		fieldType, ok := fieldProps["type"].(string)
		if !ok || fieldType != "knn_vector" {
			continue
		}

		dimValue, ok := fieldProps["dimension"]
		if !ok {
			continue
		}

		dimension, ok := dimValue.(float64)
		if !ok {
			return nil, fmt.Errorf("invalid dimension type for field %s: expected int, got %T", fieldName, dimValue)
		}

		var distance qdrant.Distance
		if spaceType, ok := fieldProps["space_type"].(string); ok {
			if mappedDistance, exists := distanceMapping[strings.ToLower(spaceType)]; exists {
				distance = mappedDistance
			}
		}

		if distance == qdrant.Distance_UnknownDistance {
			distance = qdrant.Distance_Cosine
			pterm.Warning.Printfln("Unsupported space type for field '%s', defaulting to cosine distance", fieldName)
		}

		vectorParamsMap[fieldName] = &qdrant.VectorParams{
			Size:     uint64(dimension),
			Distance: distance,
		}
	}

	return vectorParamsMap, nil
}

func (r *MigrateFromOpenSearchCmd) migrateData(ctx context.Context, sourceClient *opensearch.Client, targetClient *qdrant.Client, sourcePointCount int64) error {
	batchSize := r.Migration.BatchSize

	offsetCount := uint64(0)
	var lastSortValue any

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.OpenSearch.Index)
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

			for fieldName, value := range source {
				if vector, ok := extractOpenSearchVector(value); ok {
					vectors[fieldName] = qdrant.NewVector(vector...)
				} else {
					payload[fieldName] = value
				}
			}

			if len(vectors) > 0 {
				point.Vectors = qdrant.NewVectorsMap(vectors)
			} else {
				point.Vectors = qdrant.NewVectorsMap(map[string]*qdrant.Vector{})
			}

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
		lastSortValue = lastDoc["_id"]

		var offsetID *qdrant.PointId
		switch v := lastSortValue.(type) {
		case string:
			offsetID = qdrant.NewID(v)
		case float64:
			offsetID = qdrant.NewIDNum(uint64(v))
		case int64:
			offsetID = qdrant.NewIDNum(uint64(v))
		default:
			offsetID = qdrant.NewID(fmt.Sprintf("%v", v))
		}

		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.OpenSearch.Index, offsetID, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

func (r *MigrateFromOpenSearchCmd) searchWithPagination(ctx context.Context, client *opensearch.Client, size int, searchAfter any) ([]any, error) {
	searchRequest := map[string]any{
		"size": size,
		"sort": []map[string]any{
			{"_id": map[string]string{"order": "asc"}},
		},
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	}

	if searchAfter != nil {
		searchRequest["search_after"] = []any{searchAfter}
	}

	requestBody, err := json.Marshal(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search request: %w", err)
	}

	res, err := client.Search(
		client.Search.WithIndex(r.OpenSearch.Index),
		client.Search.WithBody(strings.NewReader(string(requestBody))),
		client.Search.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("search request failed: %w", err)
	}
	defer res.Body.Close()

	var searchResp map[string]any
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	if errorInfo, exists := searchResp["error"]; exists {
		return nil, fmt.Errorf("OpenSearch error: %v", errorInfo)
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

func extractOpenSearchVector(value any) ([]float32, bool) {
	switch v := value.(type) {
	case []any:
		vector := make([]float32, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float32:
				vector[i] = n
			case float64:
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
