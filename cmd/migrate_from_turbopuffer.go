//go:build !no_turbopuffer

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pterm/pterm"
	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/option"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

var turbopufferVectorType = regexp.MustCompile(`^\[(\d+)\]f(?:16|32)$`)

type turbopufferSource struct {
	vectors        map[string]*qdrant.VectorParams
	attributeTypes map[string]string
}

func init() {
	registerCommand[MigrateFromTurbopufferCmd]("turbopuffer", "Migrate data from a turbopuffer namespace to Qdrant.")
}

type MigrateFromTurbopufferCmd struct {
	Turbopuffer commons.TurbopufferConfig `embed:"" prefix:"turbopuffer."`
	Qdrant      commons.QdrantConfig      `embed:"" prefix:"qdrant."`
	Migration   commons.MigrationConfig   `embed:"" prefix:"migration."`
	IdField     string                    `prefix:"qdrant." help:"Field storing turbopuffer IDs in Qdrant." default:"__id__"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromTurbopufferCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}
	return nil
}

func (r *MigrateFromTurbopufferCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromTurbopufferCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("turbopuffer to Qdrant Data Migration")

	if err := r.Parse(); err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient := turbopuffer.NewClient(option.WithAPIKey(r.Turbopuffer.APIKey), option.WithRegion(r.Turbopuffer.Region))
	namespace := sourceClient.Namespace(r.Turbopuffer.Namespace)

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	if err := commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient); err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	source, err := r.describeNamespace(ctx, namespace)
	if err != nil {
		return err
	}
	sourcePointCount, err := r.countTurbopufferRows(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}
	if err := r.prepareTargetCollection(ctx, source, targetClient); err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("turbopuffer", r.Turbopuffer.Namespace, r.Qdrant.Collection)

	if err := r.migrateData(ctx, namespace, targetClient, source, sourcePointCount); err != nil {
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

	if err := commons.DeleteOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient); err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}
	return nil
}

func (r *MigrateFromTurbopufferCmd) describeNamespace(ctx context.Context, namespace turbopuffer.Namespace) (*turbopufferSource, error) {
	metadata, err := namespace.Metadata(ctx, turbopuffer.NamespaceMetadataParams{})
	if err != nil {
		return nil, fmt.Errorf("failed to get turbopuffer namespace metadata: %w", err)
	}

	source := &turbopufferSource{
		vectors:        make(map[string]*qdrant.VectorParams),
		attributeTypes: make(map[string]string, len(metadata.Schema)),
	}
	for name, attribute := range metadata.Schema {
		source.attributeTypes[name] = attribute.Type
		match := turbopufferVectorType.FindStringSubmatch(attribute.Type)
		if match == nil {
			continue
		}
		dimensions, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid dimensions for vector attribute %q: %w", name, err)
		}

		var distance qdrant.Distance
		switch attribute.Ann.DistanceMetric {
		case turbopuffer.DistanceMetricCosineDistance:
			distance = qdrant.Distance_Cosine
		case turbopuffer.DistanceMetricEuclideanSquared:
			distance = qdrant.Distance_Euclid
		case "":
			pterm.Warning.Printfln("Vector attribute %q has no distance metric, defaulting to cosine", name)
			distance = qdrant.Distance_Cosine
		default:
			return nil, fmt.Errorf("unsupported turbopuffer distance metric: %s", attribute.Ann.DistanceMetric)
		}

		source.vectors[name] = &qdrant.VectorParams{Size: dimensions, Distance: distance}
	}
	if len(source.vectors) == 0 {
		return nil, fmt.Errorf("turbopuffer namespace %q has no vector attributes", r.Turbopuffer.Namespace)
	}
	return source, nil
}

func (r *MigrateFromTurbopufferCmd) countTurbopufferRows(ctx context.Context, namespace turbopuffer.Namespace) (uint64, error) {
	var response struct {
		Aggregations struct {
			Count uint64 `json:"count"`
		} `json:"aggregations"`
	}
	_, err := namespace.Query(ctx, turbopuffer.NamespaceQueryParams{
		AggregateBy: map[string]turbopuffer.AggregateBy{"count": turbopuffer.NewAggregateByCount()},
	}, option.WithResponseBodyInto(&response))
	if err != nil {
		return 0, err
	}
	return response.Aggregations.Count, nil
}

func (r *MigrateFromTurbopufferCmd) prepareTargetCollection(ctx context.Context, source *turbopufferSource, targetClient *qdrant.Client) error {
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

	if err := targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig:  qdrant.NewVectorsConfigMap(source.vectors),
	}); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}
	pterm.Success.Printfln("Created target collection %q", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromTurbopufferCmd) migrateData(ctx context.Context, namespace turbopuffer.Namespace, targetClient *qdrant.Client, source *turbopufferSource, sourcePointCount uint64) error {
	offsetKey := "turbopuffer/" + r.Turbopuffer.Namespace
	var lastID json.RawMessage
	offsetCount := uint64(0)

	if !r.Migration.Restart {
		offsetID, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
		if offsetID != nil {
			lastID = json.RawMessage(offsetID.GetUuid())
		}
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	defer func() { _, _ = bar.Stop() }()
	displayMigrationProgress(bar, offsetCount)

	for {
		params := turbopuffer.NamespaceQueryParams{
			RankBy:            turbopuffer.NewRankByAttribute("id", turbopuffer.RankByAttributeOrderAsc),
			TopK:              turbopuffer.Int(int64(r.Migration.BatchSize)),
			IncludeAttributes: turbopuffer.IncludeAttributesParam{Bool: turbopuffer.Bool(true)},
			VectorEncoding:    turbopuffer.VectorEncodingFloat,
		}
		if lastID != nil {
			params.Filters = turbopuffer.NewFilterGt("id", lastID)
		}

		// The raw body keeps full integer precision.
		var body []byte
		if _, err := namespace.Query(ctx, params, option.WithResponseBodyInto(&body)); err != nil {
			return fmt.Errorf("failed to query turbopuffer: %w", err)
		}
		var response struct {
			Rows []map[string]any `json:"rows"`
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		if err := decoder.Decode(&response); err != nil {
			return fmt.Errorf("failed to decode turbopuffer response: %w", err)
		}
		if len(response.Rows) == 0 {
			break
		}

		points := make([]*qdrant.PointStruct, 0, len(response.Rows))
		for _, row := range response.Rows {
			point, err := r.turbopufferRowToPoint(row, source)
			if err != nil {
				return err
			}
			points = append(points, point)
		}
		if err := upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         points,
			Wait:           qdrant.PtrOf(true),
		}); err != nil {
			return err
		}
		offsetCount += uint64(len(points))
		bar.Add(len(points))

		checkpoint, err := json.Marshal(response.Rows[len(response.Rows)-1]["id"])
		if err != nil {
			return fmt.Errorf("failed to encode checkpoint: %w", err)
		}
		if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey, qdrant.NewID(string(checkpoint)), offsetCount); err != nil {
			return fmt.Errorf("failed to store checkpoint: %w", err)
		}
		lastID = checkpoint

		if len(response.Rows) < r.Migration.BatchSize {
			break
		}
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

func (r *MigrateFromTurbopufferCmd) turbopufferRowToPoint(row map[string]any, source *turbopufferSource) (*qdrant.PointStruct, error) {
	pointID, err := turbopufferPointID(row["id"])
	if err != nil {
		return nil, err
	}

	vectors := make(map[string]*qdrant.Vector)
	payload := make(map[string]any, len(row))
	for name, value := range row {
		if name == "id" {
			continue
		}
		if _, ok := source.vectors[name]; ok {
			if value == nil {
				continue
			}
			vector, err := turbopufferVector(value)
			if err != nil {
				return nil, fmt.Errorf("invalid vector attribute %q: %w", name, err)
			}
			vectors[name] = qdrant.NewVectorDense(vector)
			continue
		}
		payload[name] = convertTurbopufferValue(value, source.attributeTypes[name])
	}
	payload[r.IdField] = convertTurbopufferValue(row["id"], source.attributeTypes["id"])

	qdrantPayload, err := qdrant.TryValueMap(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to convert turbopuffer attributes: %w", err)
	}
	return &qdrant.PointStruct{
		Id:      pointID,
		Payload: qdrantPayload,
		Vectors: qdrant.NewVectorsMap(vectors),
	}, nil
}

func turbopufferPointID(id any) (*qdrant.PointId, error) {
	switch v := id.(type) {
	case json.Number:
		num, err := strconv.ParseUint(v.String(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unsupported turbopuffer ID %q: %w", v, err)
		}
		return qdrant.NewIDNum(num), nil
	case string:
		return arbitraryIDToUUID(v), nil
	default:
		return nil, fmt.Errorf("unsupported turbopuffer ID type %T", id)
	}
}

func turbopufferVector(value any) ([]float32, error) {
	elements, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected a list of numbers, got %T", value)
	}
	vector := make([]float32, len(elements))
	for i, element := range elements {
		number, ok := element.(json.Number)
		if !ok {
			return nil, fmt.Errorf("element %d is %T, expected a number", i, element)
		}
		parsed, err := strconv.ParseFloat(number.String(), 32)
		if err != nil {
			return nil, fmt.Errorf("element %d has invalid number %q: %w", i, number, err)
		}
		vector[i] = float32(parsed)
	}
	return vector, nil
}

// Uses the schema type so numbers keep one type across rows.
func convertTurbopufferValue(value any, attributeType string) any {
	switch v := value.(type) {
	case json.Number:
		if attributeType == "float" {
			f, _ := v.Float64()
			return f
		}
		if i, err := v.Int64(); err == nil {
			return i
		}
		// Qdrant payloads have no unsigned type, so integers beyond int64 become strings.
		return v.String()
	case []any:
		elementType := strings.TrimPrefix(attributeType, "[]")
		for i, element := range v {
			v[i] = convertTurbopufferValue(element, elementType)
		}
		return v
	default:
		return v
	}
}
