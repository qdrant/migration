//go:build !no_pinecone

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

// Pinecone indexes with a document schema, from API version 2026-07, are read through the documents API.
// The vector data operations reject them.
const (
	pineconeDocumentsAPIVersion = "2026-07"
	pineconeDefaultNamespace    = "__default__"
	// pineconeMaxListLimit is the largest page that the list operation returns.
	pineconeMaxListLimit = 100
	// pineconeEmbedBatch is the smallest input limit of the hosted embedding models.
	pineconeEmbedBatch = 96
)

var pineconeHTTPClient = &http.Client{Timeout: 2 * time.Minute}

type pineconeEmbedConfig struct {
	Model           string         `json:"model"`
	Dimension       uint64         `json:"dimension"`
	Metric          string         `json:"metric"`
	WriteParameters map[string]any `json:"write_parameters"`
}

type pineconeIndexDescription struct {
	Schema *struct {
		Fields map[string]struct {
			Type           string               `json:"type"`
			Dimension      uint64               `json:"dimension"`
			Metric         string               `json:"metric"`
			FullTextSearch any                  `json:"full_text_search"`
			Embed          *pineconeEmbedConfig `json:"embed"`
			SparseEmbed    *pineconeEmbedConfig `json:"sparse_embed"`
		} `json:"fields"`
	} `json:"schema"`
}

// pineconeVectorField is a Qdrant vector from a vector field of the document, or from the text of a field
// that Pinecone embeds itself.
type pineconeVectorField struct {
	Field  string
	Name   string
	Sparse bool
	Embed  *pineconeEmbedConfig
	Size   uint64
	Metric string
}

type pineconeDocumentPlan struct {
	Vectors []pineconeVectorField
	Text    []string
}

// documentPlan returns nil for an index without a document schema, which the vectors API serves.
// Field names that start with "_" are reserved, and a vector index has only those.
func (r *MigrateFromPineconeCmd) documentPlan(description *pineconeIndexDescription) *pineconeDocumentPlan {
	if description.Schema == nil {
		return nil
	}
	plan := &pineconeDocumentPlan{}
	for name, field := range description.Schema.Fields {
		if strings.HasPrefix(name, "_") {
			continue
		}
		switch field.Type {
		case "dense_vector":
			plan.Vectors = append(plan.Vectors, pineconeVectorField{Field: name, Name: name, Size: field.Dimension, Metric: field.Metric})
		case "sparse_vector":
			plan.Vectors = append(plan.Vectors, pineconeVectorField{Field: name, Name: name, Sparse: true})
		case "string":
			if field.FullTextSearch != nil {
				plan.Text = append(plan.Text, name)
			}
			if field.Embed != nil {
				plan.Vectors = append(plan.Vectors, pineconeVectorField{Field: name, Name: name, Embed: field.Embed, Size: field.Embed.Dimension, Metric: field.Embed.Metric})
			}
			if field.SparseEmbed != nil {
				// Qdrant rejects a dense and a sparse vector with the same name.
				sparseName := name
				if field.Embed != nil {
					sparseName += "_sparse"
				}
				plan.Vectors = append(plan.Vectors, pineconeVectorField{Field: name, Name: sparseName, Sparse: true, Embed: field.SparseEmbed})
			}
		}
	}
	if len(plan.Vectors) == 0 && len(plan.Text) == 0 {
		return nil
	}
	return plan
}

func (r *MigrateFromPineconeCmd) describeIndex(ctx context.Context) (*pineconeIndexDescription, error) {
	var description pineconeIndexDescription
	path := "/indexes/" + url.PathEscape(r.Pinecone.IndexName)
	if err := r.pineconeRequest(ctx, http.MethodGet, r.controlPlaneURL()+path, nil, &description); err != nil {
		return nil, fmt.Errorf("failed to describe the Pinecone index: %w", err)
	}
	return &description, nil
}

func (r *MigrateFromPineconeCmd) prepareDocumentCollection(ctx context.Context, plan *pineconeDocumentPlan, targetClient *qdrant.Client) error {
	if !r.Migration.CreateCollection {
		return nil
	}
	exists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if exists {
		pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}

	distances := map[string]qdrant.Distance{
		"cosine":     qdrant.Distance_Cosine,
		"euclidean":  qdrant.Distance_Euclid,
		"dotproduct": qdrant.Distance_Dot,
	}
	dense := map[string]*qdrant.VectorParams{}
	sparse := map[string]*qdrant.SparseVectorParams{}
	for _, v := range plan.Vectors {
		if v.Sparse {
			sparse[v.Name] = &qdrant.SparseVectorParams{}
		} else {
			dense[v.Name] = &qdrant.VectorParams{Size: v.Size, Distance: distances[v.Metric]}
		}
	}

	request := &qdrant.CreateCollection{CollectionName: r.Qdrant.Collection}
	if len(dense) > 0 {
		request.VectorsConfig = qdrant.NewVectorsConfigMap(dense)
	}
	if len(sparse) > 0 {
		request.SparseVectorsConfig = qdrant.NewSparseVectorsConfig(sparse)
	}
	if err := targetClient.CreateCollection(ctx, request); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}

	// Full-text fields keep keyword search in Qdrant through a text index.
	for _, field := range plan.Text {
		_, err := targetClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
			CollectionName: r.Qdrant.Collection,
			FieldName:      field,
			FieldType:      qdrant.FieldType_FieldTypeText.Enum(),
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return fmt.Errorf("failed to create a text index on %q: %w", field, err)
		}
	}

	pterm.Success.Printfln("Created target collection '%s'", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromPineconeCmd) migrateDocuments(ctx context.Context, plan *pineconeDocumentPlan, targetClient *qdrant.Client, sourcePointCount uint64) error {
	namespace := r.Pinecone.Namespace
	if namespace == "" {
		namespace = pineconeDefaultNamespace
	}
	documentsURL := pineconeBaseURL(r.Pinecone.IndexHost) + "/namespaces/" + url.PathEscape(namespace) + "/documents/"

	pageToken := ""
	offsetCount := uint64(0)
	if !r.Migration.Restart {
		id, stored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.progressKey())
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		pageToken = id.GetUuid()
		offsetCount = stored
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		listBody := map[string]any{"limit": min(r.Migration.BatchSize, pineconeMaxListLimit)}
		if pageToken != "" {
			listBody["pagination_token"] = pageToken
		}
		var list struct {
			Documents []struct {
				ID string `json:"_id"`
			} `json:"documents"`
			Pagination struct {
				Next string `json:"next"`
			} `json:"pagination"`
		}
		if err := r.pineconeRequest(ctx, http.MethodPost, documentsURL+"list", listBody, &list); err != nil {
			return fmt.Errorf("failed to list documents from Pinecone: %w", err)
		}
		if len(list.Documents) == 0 {
			break
		}

		ids := make([]string, len(list.Documents))
		for i, document := range list.Documents {
			ids[i] = document.ID
		}
		var fetched struct {
			Documents map[string]map[string]any `json:"documents"`
		}
		if err := r.pineconeRequest(ctx, http.MethodPost, documentsURL+"fetch", map[string]any{"ids": ids}, &fetched); err != nil {
			return fmt.Errorf("failed to fetch documents from Pinecone: %w", err)
		}

		points, err := r.documentsToPoints(ctx, plan, ids, fetched.Documents)
		if err != nil {
			return err
		}
		if len(points) > 0 {
			err = upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
				CollectionName: r.Qdrant.Collection,
				Points:         points,
				Wait:           qdrant.PtrOf(true),
			})
			if err != nil {
				return err
			}
		}

		offsetCount += uint64(len(points))
		pageToken = list.Pagination.Next
		if pageToken != "" {
			if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.progressKey(), qdrant.NewID(pageToken), offsetCount); err != nil {
				return fmt.Errorf("failed to store offset: %w", err)
			}
		}
		bar.Add(len(points))

		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
		if pageToken == "" {
			break
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// documentsToPoints keeps the fields of each document as payload and copies its vector fields.
// With --pinecone.embed pinecone, it also embeds the text of the fields that Pinecone embedded itself.
func (r *MigrateFromPineconeCmd) documentsToPoints(ctx context.Context, plan *pineconeDocumentPlan, ids []string, documents map[string]map[string]any) ([]*qdrant.PointStruct, error) {
	vectorNames := map[string]string{}
	for _, v := range plan.Vectors {
		if v.Embed == nil {
			vectorNames[v.Field] = v.Name
		}
	}

	points := make([]*qdrant.PointStruct, 0, len(ids))
	vectorsOf := make([]map[string]*qdrant.Vector, 0, len(ids))
	for _, id := range ids {
		document, ok := documents[id]
		if !ok {
			// The document was deleted between list and fetch.
			continue
		}
		payload := map[string]any{}
		vectors := map[string]*qdrant.Vector{}
		for key, value := range document {
			name, isVector := vectorNames[key]
			switch {
			case key == "_id", isVector && value == nil:
			case isVector:
				vector, err := pineconeFieldVector(value)
				if err != nil {
					return nil, fmt.Errorf("invalid vector field %q of document %q: %w", key, id, err)
				}
				vectors[name] = vector
			default:
				payload[key] = value
			}
		}
		payload[r.IdField] = id

		qdrantPayload, err := qdrant.TryValueMap(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to convert the fields of document %q: %w", id, err)
		}
		points = append(points, &qdrant.PointStruct{Id: arbitraryIDToUUID(id), Payload: qdrantPayload})
		vectorsOf = append(vectorsOf, vectors)
	}

	for _, v := range plan.Vectors {
		if v.Embed == nil || r.Pinecone.Embed != "pinecone" {
			continue
		}
		var texts []string
		var owners []int
		for i, point := range points {
			if text := point.Payload[v.Field].GetStringValue(); text != "" {
				texts = append(texts, text)
				owners = append(owners, i)
			}
		}
		embedded, err := r.embedTexts(ctx, v, texts)
		if err != nil {
			return nil, err
		}
		for j, vector := range embedded {
			vectorsOf[owners[j]][v.Name] = vector
		}
	}

	for i, point := range points {
		point.Vectors = qdrant.NewVectorsMap(vectorsOf[i])
	}
	return points, nil
}

func pineconeFieldVector(value any) (*qdrant.Vector, error) {
	switch v := value.(type) {
	case []any:
		values := make([]float32, len(v))
		for i, item := range v {
			number, ok := item.(float64)
			if !ok {
				return nil, errors.New("expected a list of numbers")
			}
			values[i] = float32(number)
		}
		return qdrant.NewVectorDense(values), nil
	case map[string]any:
		data, _ := json.Marshal(v)
		var sparse struct {
			Indices []uint32  `json:"indices"`
			Values  []float32 `json:"values"`
		}
		if err := json.Unmarshal(data, &sparse); err != nil {
			return nil, err
		}
		return qdrant.NewVectorSparse(sparse.Indices, sparse.Values), nil
	}
	return nil, fmt.Errorf("unsupported value of type %T", value)
}

// embedTexts uses Pinecone's hosted model with the write parameters of the field, so the vectors
// match the ones that Pinecone stored.
func (r *MigrateFromPineconeCmd) embedTexts(ctx context.Context, v pineconeVectorField, texts []string) ([]*qdrant.Vector, error) {
	parameters := map[string]any{}
	for key, value := range v.Embed.WriteParameters {
		parameters[key] = value
	}
	if !v.Sparse && v.Embed.Dimension > 0 {
		parameters["dimension"] = v.Embed.Dimension
	}

	vectors := make([]*qdrant.Vector, 0, len(texts))
	for start := 0; start < len(texts); start += pineconeEmbedBatch {
		end := min(start+pineconeEmbedBatch, len(texts))
		inputs := make([]map[string]string, 0, end-start)
		for _, text := range texts[start:end] {
			inputs = append(inputs, map[string]string{"text": text})
		}
		var response struct {
			Data []struct {
				Values        []float32 `json:"values"`
				SparseIndices []uint32  `json:"sparse_indices"`
				SparseValues  []float32 `json:"sparse_values"`
			} `json:"data"`
		}
		body := map[string]any{"model": v.Embed.Model, "parameters": parameters, "inputs": inputs}
		if err := r.pineconeRequest(ctx, http.MethodPost, r.controlPlaneURL()+"/embed", body, &response); err != nil {
			var limit *pineconeLimitError
			if errors.As(err, &limit) {
				return nil, fmt.Errorf("pinecone does not return the vectors that it generates from '%s', and it refused to embed the text again: %s. "+
					"Wait for the limit to reset or raise it, or pass --pinecone.embed none to migrate the documents without these vectors", v.Field, strings.TrimSuffix(limit.Message, "."))
			}
			return nil, fmt.Errorf("failed to embed '%s' with %s: %w", v.Field, v.Embed.Model, err)
		}
		if len(response.Data) != end-start {
			return nil, fmt.Errorf("pinecone returned %d embeddings for %d texts", len(response.Data), end-start)
		}
		for _, item := range response.Data {
			if v.Sparse {
				vectors = append(vectors, qdrant.NewVectorSparse(item.SparseIndices, item.SparseValues))
			} else {
				vectors = append(vectors, qdrant.NewVectorDense(item.Values))
			}
		}
	}
	return vectors, nil
}

func (r *MigrateFromPineconeCmd) controlPlaneURL() string {
	if r.Pinecone.ServiceHost != "" {
		return pineconeBaseURL(r.Pinecone.ServiceHost)
	}
	return "https://api.pinecone.io"
}

// pineconeBaseURL adds https to a host without a scheme, as the Pinecone client does.
func pineconeBaseURL(host string) string {
	if !strings.Contains(host, "://") {
		host = "https://" + host
	}
	return strings.TrimRight(host, "/")
}

// pineconeLimitError is a quota that a retry cannot fix, such as the monthly limit of embedding tokens.
type pineconeLimitError struct {
	Message string
}

func (e *pineconeLimitError) Error() string { return e.Message }

// pineconeRequest retries rate limits and server errors with exponential backoff.
func (r *MigrateFromPineconeCmd) pineconeRequest(ctx context.Context, method, target string, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}

	var err error
	for attempt := 0; attempt < 8; attempt++ {
		var retryable bool
		retryable, err = r.pineconeRequestOnce(ctx, method, target, payload, out)
		if err == nil || !retryable {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(baseRetryDelay*time.Duration(1<<attempt), 30*time.Second)):
		}
	}
	return err
}

func (r *MigrateFromPineconeCmd) pineconeRequestOnce(ctx context.Context, method, target string, payload []byte, out any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
	if err != nil {
		return false, err
	}
	req.Header.Set("Api-Key", r.Pinecone.APIKey)
	req.Header.Set("X-Pinecone-API-Version", pineconeDocumentsAPIVersion)
	req.Header.Set("Content-Type", "application/json")

	res, err := pineconeHTTPClient.Do(req)
	if err != nil {
		// Pinecone may have embedded the texts of a request whose response was lost, so a retry uses the quota again.
		return !strings.HasSuffix(req.URL.Path, "/embed"), err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return true, err
	}

	if res.StatusCode >= 300 {
		var parsed struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		message := strings.TrimSpace(string(data))
		if json.Unmarshal(data, &parsed) == nil && parsed.Error.Message != "" {
			message = parsed.Error.Message
		}
		if res.StatusCode == http.StatusTooManyRequests && strings.Contains(message, "for the current month") {
			return false, &pineconeLimitError{Message: message}
		}
		retryable := res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= http.StatusInternalServerError
		return retryable, fmt.Errorf("%s %s returned status %d: %s", method, req.URL.Path, res.StatusCode, message)
	}
	if out == nil {
		return false, nil
	}
	return false, json.Unmarshal(data, out)
}
