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
	"sort"
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

type pineconeEmbedConfig struct {
	Model           string         `json:"model"`
	Dimension       uint64         `json:"dimension"`
	Metric          string         `json:"metric"`
	WriteParameters map[string]any `json:"write_parameters"`
}

type pineconeField struct {
	Type           string               `json:"type"`
	Dimension      uint64               `json:"dimension"`
	Metric         string               `json:"metric"`
	FullTextSearch json.RawMessage      `json:"full_text_search"`
	Embed          *pineconeEmbedConfig `json:"embed"`
	SparseEmbed    *pineconeEmbedConfig `json:"sparse_embed"`
}

type pineconeIndexDescription struct {
	Schema *struct {
		Fields map[string]pineconeField `json:"fields"`
	} `json:"schema"`
}

// pineconeVectorField describes where a Qdrant vector comes from: a vector field of the document,
// or the text of a field that Pinecone embeds itself.
type pineconeVectorField struct {
	Field  string
	Name   string
	Sparse bool
	Embed  *pineconeEmbedConfig
	Size   uint64
	Metric string
}

type pineconeDocumentPlan struct {
	Vectors  []pineconeVectorField
	Text     []string
	Embedded bool
}

// documentPlan returns nil for an index without a document schema, which the vectors API serves.
func (r *MigrateFromPineconeCmd) documentPlan(description *pineconeIndexDescription) *pineconeDocumentPlan {
	if description.Schema == nil {
		return nil
	}
	names := make([]string, 0, len(description.Schema.Fields))
	for name := range description.Schema.Fields {
		names = append(names, name)
	}
	sort.Strings(names)

	plan := &pineconeDocumentPlan{}
	var dense, sparse []pineconeVectorField
	isDocumentSchema := false
	for _, name := range names {
		field := description.Schema.Fields[name]
		switch field.Type {
		case "dense_vector":
			dense = append(dense, pineconeVectorField{Field: name, Size: field.Dimension, Metric: field.Metric})
			isDocumentSchema = isDocumentSchema || name != "_values"
		case "sparse_vector":
			sparse = append(sparse, pineconeVectorField{Field: name, Sparse: true})
			isDocumentSchema = isDocumentSchema || name != "_sparse_values"
		case "string":
			if len(field.FullTextSearch) > 0 && string(field.FullTextSearch) != "null" {
				plan.Text = append(plan.Text, name)
				isDocumentSchema = true
			}
			if field.Embed != nil {
				dense = append(dense, pineconeVectorField{Field: name, Embed: field.Embed, Size: field.Embed.Dimension, Metric: field.Embed.Metric})
				isDocumentSchema = true
			}
			if field.SparseEmbed != nil {
				sparse = append(sparse, pineconeVectorField{Field: name, Sparse: true, Embed: field.SparseEmbed})
				isDocumentSchema = true
			}
		}
	}
	if !isDocumentSchema {
		return nil
	}

	// Each vector takes the name of its field. Qdrant rejects a dense and a sparse vector with the same name,
	// so a sparse vector from a field that also gives a dense vector gets the suffix "_sparse".
	denseFields := map[string]bool{}
	for _, v := range dense {
		v.Name = v.Field
		denseFields[v.Field] = true
		plan.Vectors = append(plan.Vectors, v)
	}
	for _, v := range sparse {
		v.Name = v.Field
		if denseFields[v.Field] {
			v.Name = v.Field + "_sparse"
		}
		plan.Vectors = append(plan.Vectors, v)
	}
	for _, v := range plan.Vectors {
		plan.Embedded = plan.Embedded || v.Embed != nil
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
			continue
		}
		distance, ok := distances[strings.ToLower(v.Metric)]
		if !ok {
			distance = qdrant.Distance_Cosine
		}
		dense[v.Name] = &qdrant.VectorParams{Size: v.Size, Distance: distance}
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
	batchSize := min(r.Migration.BatchSize, pineconeMaxListLimit)
	namespace := r.Pinecone.Namespace
	if namespace == "" {
		namespace = pineconeDefaultNamespace
	}
	documentsURL := pineconeBaseURL(r.Pinecone.IndexHost) + "/namespaces/" + url.PathEscape(namespace) + "/documents/"

	for _, v := range plan.Vectors {
		if v.Embed != nil && r.Pinecone.Embed == "pinecone" {
			pterm.Info.Printfln("Pinecone does not return the vectors that it generates from '%s', so the tool embeds the text again with %s.", v.Field, v.Embed.Model)
		}
	}

	pageToken := ""
	offsetCount := uint64(0)
	if !r.Migration.Restart {
		id, stored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Pinecone.IndexHost)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		if id != nil {
			pageToken = id.GetUuid()
		}
		offsetCount = stored
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)
	warnedMissing := map[string]bool{}

	for {
		listBody := map[string]any{"limit": batchSize}
		if pageToken != "" {
			listBody["pagination_token"] = pageToken
		}
		var list struct {
			Documents []struct {
				ID string `json:"_id"`
			} `json:"documents"`
			Pagination *struct {
				Next string `json:"next"`
			} `json:"pagination"`
		}
		if err := r.pineconeRequest(ctx, http.MethodPost, documentsURL+"list", listBody, &list); err != nil {
			return fmt.Errorf("failed to list documents from Pinecone: %w", err)
		}
		if len(list.Documents) == 0 {
			break
		}

		ids := make([]string, 0, len(list.Documents))
		for _, document := range list.Documents {
			ids = append(ids, document.ID)
		}
		var fetched struct {
			Documents map[string]map[string]any `json:"documents"`
		}
		if err := r.pineconeRequest(ctx, http.MethodPost, documentsURL+"fetch", map[string]any{"ids": ids}, &fetched); err != nil {
			return fmt.Errorf("failed to fetch documents from Pinecone: %w", err)
		}

		points, err := r.documentsToPoints(ctx, plan, ids, fetched.Documents, warnedMissing)
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
		next := ""
		if list.Pagination != nil {
			next = list.Pagination.Next
		}
		if next != "" {
			if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Pinecone.IndexHost, qdrant.NewID(next), offsetCount); err != nil {
				return fmt.Errorf("failed to store offset: %w", err)
			}
		}
		bar.Add(len(points))

		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
		if next == "" {
			break
		}
		pageToken = next
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// documentsToPoints keeps the fields of each document as payload, copies its vector fields, and embeds
// the text of the fields that Pinecone embedded itself.
func (r *MigrateFromPineconeCmd) documentsToPoints(ctx context.Context, plan *pineconeDocumentPlan, ids []string, documents map[string]map[string]any, warnedMissing map[string]bool) ([]*qdrant.PointStruct, error) {
	vectorFields := map[string]bool{}
	for _, v := range plan.Vectors {
		if v.Embed == nil {
			vectorFields[v.Field] = true
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
			switch {
			case key == "_id":
			case vectorFields[key] && value == nil:
				// A null vector is a missing vector.
			case vectorFields[key]:
				vector, err := pineconeFieldVector(value)
				if err != nil {
					return nil, fmt.Errorf("invalid vector field %q of document %q: %w", key, id, err)
				}
				for _, v := range plan.Vectors {
					if v.Field == key && v.Embed == nil {
						vectors[v.Name] = vector
					}
				}
			default:
				payload[key] = value
			}
		}
		if _, exists := payload[r.IdField]; exists {
			return nil, fmt.Errorf("document %q has a field named %q, set --qdrant.id-field to another name", id, r.IdField)
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
		if v.Embed == nil {
			for i := range points {
				if vectorsOf[i][v.Name] == nil && !warnedMissing[v.Field] {
					pterm.Warning.Printfln("Some documents have no '%s' vector. Their points have no '%s' vector.", v.Field, v.Name)
					warnedMissing[v.Field] = true
				}
			}
			continue
		}
		if r.Pinecone.Embed == "none" {
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

	// An empty map is valid: Qdrant stores a point without vectors.
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
		if err == nil || !retryable || ctx.Err() != nil {
			return err
		}
		delay := min(baseRetryDelay*time.Duration(1<<attempt), 30*time.Second)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}

func (r *MigrateFromPineconeCmd) pineconeRequestOnce(ctx context.Context, method, target string, payload []byte, out any) (bool, error) {
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return false, err
	}
	req.Header.Set("Api-Key", r.Pinecone.APIKey)
	req.Header.Set("X-Pinecone-API-Version", pineconeDocumentsAPIVersion)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 2 * time.Minute}
	res, err := client.Do(req)
	if err != nil {
		return true, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return true, err
	}

	if res.StatusCode >= 300 {
		message := strings.TrimSpace(string(data))
		var parsed struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
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
