package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pterm/pterm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/qdrant/go-client/qdrant"
	"github.com/qdrant/go-client/qdrant/serverless"

	"github.com/qdrant/migration/pkg/commons"
)

const (
	// SAMPLE_SIZE_PER_WORKER is the number of points to sample per worker to determine ranges for parallel migration.
	SAMPLE_SIZE_PER_WORKER = 10
)

func init() {
	registerCommand[MigrateFromQdrantCmd]("qdrant", "Migrate data from a Qdrant database or Qdrant Serverless to Qdrant or Qdrant Serverless.")
}

type MigrateFromQdrantCmd struct {
	Source               commons.QdrantConfig    `embed:"" prefix:"source."`
	Target               commons.QdrantConfig    `embed:"" prefix:"target."`
	Migration            commons.MigrationConfig `embed:"" prefix:"migration."`
	MaxMessageSize       int                     `help:"Maximum gRPC message size in bytes (default: 33554432 = 32MB)" default:"33554432" prefix:"source."`
	EnsurePayloadIndexes bool                    `help:"Ensure payload indexes from the source are created on the target" default:"true" prefix:"target."`
	ShardKey             string                  `help:"Fixed keyword shard key used for every target upsert" prefix:"target."`
	ShardKeyField        string                  `help:"Payload field used as the target shard key for each point" prefix:"target."`
	NumWorkers           int                     `help:"Number of parallel workers for data migration (0 = number of CPU cores)" default:"0" prefix:"migration."`

	sourceHost string
	sourcePort int
	sourceTLS  bool
	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromQdrantCmd) Parse() error {
	var err error
	r.sourceHost, r.sourcePort, r.sourceTLS, err = parseQdrantUrl(r.Source.Url)
	if err != nil {
		return fmt.Errorf("failed to parse source URL: %w", err)
	}
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Target.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}
	// The serverless client connects over TLS on port 443 whatever the URL says.
	// The same port here lets ValidateParsedValues compare the endpoints in use.
	if r.Source.Serverless {
		r.sourcePort, r.sourceTLS = DefaultHTTPSPort, true
	}
	if r.Target.Serverless {
		r.targetPort, r.targetTLS = DefaultHTTPSPort, true
	}
	return nil
}

func (r *MigrateFromQdrantCmd) Validate() error {
	if r.ShardKey != "" && r.ShardKeyField != "" {
		return fmt.Errorf("target shard-key and shard-key-field cannot be used together")
	}
	if r.Target.Serverless && r.routesToTargetShard() {
		return fmt.Errorf("target shard-key and shard-key-field cannot be used with a Qdrant Serverless target")
	}
	if r.Source.Serverless && r.Migration.BatchSize > serverlessMaxLimit {
		return fmt.Errorf("batch size must be at most %d with a Qdrant Serverless source, got: %d", serverlessMaxLimit, r.Migration.BatchSize)
	}
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromQdrantCmd) routesToTargetShard() bool {
	return r.ShardKey != "" || r.ShardKeyField != ""
}

func (r *MigrateFromQdrantCmd) ValidateParsedValues() error {
	if r.sourceHost == r.targetHost && r.sourcePort == r.targetPort && r.Source.Collection == r.Target.Collection {
		return fmt.Errorf("source and target collections must be different")
	}
	return nil
}

func (r *MigrateFromQdrantCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Qdrant Data Migration")

	if err := r.Parse(); err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}
	if err := r.ValidateParsedValues(); err != nil {
		return fmt.Errorf("failed to validate input: %w", err)
	}

	// Default to the number of CPU cores if NumWorkers is not specified.
	if r.NumWorkers == 0 {
		r.NumWorkers = runtime.NumCPU()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := connectToQdrantOrServerless(globals, r.sourceHost, r.sourcePort, r.Source.APIKey, r.sourceTLS, r.Source.Serverless, r.MaxMessageSize)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceClient.Close()

	targetClient, err := connectToQdrantOrServerless(globals, r.targetHost, r.targetPort, r.Target.APIKey, r.targetTLS, r.Target.Serverless, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetClient.Close()

	if err := targetClient.prepareOffsetsCollection(ctx, r.Migration.OffsetsCollection); err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Source.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	if err := r.prepareTargetCollection(ctx, sourceClient, r.Source.Collection, targetClient, r.Target.Collection); err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	sourceProvider := "qdrant"
	if r.Source.Serverless {
		sourceProvider = "qdrant-serverless"
	}
	displayMigrationStart(sourceProvider, r.Source.Collection, r.Target.Collection)

	if err := r.migrateData(ctx, sourceClient, r.Source.Collection, targetClient, r.Target.Collection, sourcePointCount); err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	if err := verifyTargetCount(ctx, targetClient, r.Target.Collection, sourcePointCount, r.Migration.OffsetsCollection); err != nil {
		return err
	}

	err = targetClient.deleteOffsetsCollection(ctx, r.Migration.OffsetsCollection)
	if err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}

	return nil
}

// prepareTargetCollection ensures the target collection exists and is configured correctly.
// It can create the collection based on the source's schema and create payload indexes.
func (r *MigrateFromQdrantCmd) prepareTargetCollection(ctx context.Context, sourceClient *qdrantConnection, sourceCollection string, targetClient *qdrantConnection, targetCollection string) error {
	if targetClient.serverless != nil {
		return r.prepareServerlessTargetCollection(ctx, sourceClient, sourceCollection, targetClient.serverless, targetCollection)
	}

	request, payloadSchema, err := clusterSourceConfig(ctx, sourceClient, sourceCollection, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get source collection info: %w", err)
	}

	if r.Migration.CreateCollection {
		exists, err := targetClient.cluster.CollectionExists(ctx, targetCollection)
		if err != nil {
			return fmt.Errorf("failed to check if collection exists: %w", err)
		}
		if exists {
			fmt.Print("\n")
			pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", targetCollection)
		} else {
			if r.routesToTargetShard() {
				request.ShardingMethod = qdrant.PtrOf(qdrant.ShardingMethod_Custom)
			}
			if err := targetClient.cluster.CreateCollection(ctx, request); err != nil {
				return fmt.Errorf("failed to create target collection: %w", err)
			}
		}
	}
	return r.ensureTargetPayloadIndexes(ctx, payloadSchema, targetClient.cluster, targetCollection)
}

// createCollectionFromInfo returns a request that creates a collection with the configuration of a Qdrant cluster collection.
func createCollectionFromInfo(collection string, info *qdrant.CollectionInfo) *qdrant.CreateCollection {
	params := info.GetConfig().GetParams()
	return &qdrant.CreateCollection{
		CollectionName:         collection,
		HnswConfig:             info.GetConfig().GetHnswConfig(),
		WalConfig:              info.GetConfig().GetWalConfig(),
		OptimizersConfig:       info.GetConfig().GetOptimizerConfig(),
		ShardNumber:            &params.ShardNumber,
		OnDiskPayload:          &params.OnDiskPayload, //nolint:staticcheck // Deprecated, kept for backward compatibility.
		Payload:                params.GetPayload(),
		IdTracker:              params.GetIdTracker(),
		VectorsConfig:          params.VectorsConfig,
		ReplicationFactor:      params.ReplicationFactor,
		WriteConsistencyFactor: params.WriteConsistencyFactor,
		QuantizationConfig:     info.GetConfig().GetQuantizationConfig(),
		ShardingMethod:         params.ShardingMethod,
		SparseVectorsConfig:    params.SparseVectorsConfig,
		StrictModeConfig:       info.GetConfig().GetStrictModeConfig(),
		Metadata:               info.GetConfig().GetMetadata(),
	}
}

// prepareServerlessTargetCollection creates a serverless collection with the vectors and payload indexes of the source collection.
// Qdrant Serverless sets payload indexes only when it creates a collection.
func (r *MigrateFromQdrantCmd) prepareServerlessTargetCollection(ctx context.Context, sourceClient *qdrantConnection, sourceCollection string, targetClient *serverless.Client, targetCollection string) error {
	sourceConfig, err := serverlessSourceConfig(ctx, sourceClient, sourceCollection)
	if err != nil {
		return fmt.Errorf("failed to get source collection info: %w", err)
	}
	if !r.EnsurePayloadIndexes {
		sourceConfig.PayloadIndexes = nil
	}

	if r.Migration.CreateCollection {
		created, err := createServerlessCollection(ctx, targetClient, targetCollection, sourceConfig)
		// Qdrant Serverless rejects payload indexes that it does not support, for example too many of them.
		if status.Code(err) == codes.InvalidArgument && len(sourceConfig.GetPayloadIndexes()) > 0 {
			return fmt.Errorf("failed to create target collection: %w. If the error is about the payload indexes, use --target.ensure-payload-indexes=false to create the collection without them", err)
		}
		if err != nil {
			return fmt.Errorf("failed to create target collection: %w", err)
		}
		if created {
			return nil
		}
		fmt.Print("\n")
		pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", targetCollection)
	}
	if len(sourceConfig.GetPayloadIndexes()) == 0 {
		return nil
	}

	targetConfig, err := getServerlessCollectionConfig(ctx, targetClient, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get target collection information: %w", err)
	}
	for name := range sourceConfig.GetPayloadIndexes() {
		if _, ok := targetConfig.GetPayloadIndexes()[name]; !ok {
			pterm.Warning.Printfln("The target collection has no payload index on '%s'. Qdrant Serverless sets payload indexes only when it creates a collection.", name)
		}
	}
	return nil
}

// clusterSourceConfig returns a request that creates the source collection on a Qdrant cluster as the target collection,
// and the payload schema of the source collection.
func clusterSourceConfig(ctx context.Context, sourceClient *qdrantConnection, sourceCollection, targetCollection string) (*qdrant.CreateCollection, map[string]*qdrant.PayloadSchemaInfo, error) {
	if sourceClient.serverless != nil {
		config, err := getServerlessCollectionConfig(ctx, sourceClient.serverless, sourceCollection)
		if err != nil {
			return nil, nil, err
		}
		request, err := createCollectionFromServerless(targetCollection, config)
		if err != nil {
			return nil, nil, err
		}
		return request, payloadSchemaFromServerless(config.GetPayloadIndexes()), nil
	}
	info, err := sourceClient.cluster.GetCollectionInfo(ctx, sourceCollection)
	if err != nil {
		return nil, nil, err
	}
	return createCollectionFromInfo(targetCollection, info), info.GetPayloadSchema(), nil
}

// serverlessSourceConfig returns the configuration of the source collection in the serverless format.
func serverlessSourceConfig(ctx context.Context, sourceClient *qdrantConnection, sourceCollection string) (*serverless.CollectionConfig, error) {
	if sourceClient.serverless != nil {
		return getServerlessCollectionConfig(ctx, sourceClient.serverless, sourceCollection)
	}
	info, err := sourceClient.cluster.GetCollectionInfo(ctx, sourceCollection)
	if err != nil {
		return nil, err
	}
	return serverlessConfigFromCollectionInfo(info)
}

// ensureTargetPayloadIndexes checks the sharding of a Qdrant cluster target and creates the source payload indexes that the target is missing.
func (r *MigrateFromQdrantCmd) ensureTargetPayloadIndexes(ctx context.Context, sourcePayloadSchema map[string]*qdrant.PayloadSchemaInfo, targetClient *qdrant.Client, targetCollection string) error {
	targetCollectionInfo, err := targetClient.GetCollectionInfo(ctx, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get target collection information: %w", err)
	}
	if r.routesToTargetShard() && targetCollectionInfo.GetConfig().GetParams().GetShardingMethod() != qdrant.ShardingMethod_Custom {
		return fmt.Errorf("target collection %q must use custom sharding when a target shard key is configured", targetCollection)
	}

	// If EnsurePayloadIndexes is enabled, create any missing payload indexes on the target.
	if r.EnsurePayloadIndexes {
		for name, schemaInfo := range sourcePayloadSchema {
			fieldType := getFieldType(schemaInfo.GetDataType())
			if fieldType == nil {
				continue
			}

			// If there is already an index in the target collection, skip.
			if _, ok := targetCollectionInfo.GetPayloadSchema()[name]; ok {
				continue
			}

			if _, err := targetClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
				CollectionName:   targetCollection,
				FieldName:        name,
				FieldType:        fieldType,
				FieldIndexParams: schemaInfo.GetParams(),
				Wait:             qdrant.PtrOf(true),
			}); err != nil {
				return fmt.Errorf("failed creating index on target collection: %w", err)
			}
		}
	}
	return nil
}

// getFieldType converts a Qdrant PayloadSchemaType to a FieldType used for creating indexes.
func getFieldType(dataType qdrant.PayloadSchemaType) *qdrant.FieldType {
	switch dataType {
	case qdrant.PayloadSchemaType_Keyword:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeKeyword)
	case qdrant.PayloadSchemaType_Integer:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeInteger)
	case qdrant.PayloadSchemaType_Float:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeFloat)
	case qdrant.PayloadSchemaType_Geo:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeGeo)
	case qdrant.PayloadSchemaType_Text:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeText)
	case qdrant.PayloadSchemaType_Bool:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeBool)
	case qdrant.PayloadSchemaType_Datetime:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeDatetime)
	case qdrant.PayloadSchemaType_Uuid:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeUuid)
	}
	return nil
}

// rangeSpec defines a range of points for a worker to process in parallel migration.
// It has a start and an optional end point ID.
type rangeSpec struct {
	id    int
	start *qdrant.PointId
	end   *qdrant.PointId
	// offsetKey identifies this range in the offsets collection. It embeds a fingerprint of the
	// range boundaries, so a stored offset is only ever applied to the exact range it was saved for.
	offsetKey string
}

// comparePointIDs returns true if a < b.
// Usually Qdrant collections use either numeric or UUID IDs, not both.
// If mixed, numeric IDs (a.GetUuid() == "") sort before UUIDs since "" < any non-empty string.
func comparePointIDs(a, b *qdrant.PointId) bool {
	if a == nil {
		return b != nil
	}
	if b == nil {
		return false
	}
	aStr, bStr := a.GetUuid(), b.GetUuid()
	if aStr != "" || bStr != "" {
		return aStr < bStr
	}
	return a.GetNum() < b.GetNum()
}

// convertVector converts a VectorOutput from a retrieved point to a Vector for upserting.
// Returns nil if the vector is absent, empty, or of an unrecognized type.
func convertVector(v *qdrant.VectorOutput) *qdrant.Vector {
	if v == nil {
		return nil
	}
	if sparse := v.GetSparseVector(); sparse != nil {
		return qdrant.NewVectorSparse(sparse.GetIndices(), sparse.GetValues())
	}
	if multi := v.GetMultiVector(); multi != nil {
		return &qdrant.Vector{Vector: &qdrant.Vector_MultiDense{MultiDense: multi}}
	}
	// Important: this should be a fallback option, as everything can be interpreted as dense vectors.
	if dense := v.GetDenseVector(); dense != nil {
		data := dense.GetData()
		if len(data) == 0 {
			return nil
		}
		return qdrant.NewVectorDense(data)
	}
	return nil
}

// convertVectors converts the vector data from a retrieved point to a format suitable for upserting.
func convertVectors(p *qdrant.RetrievedPoint) *qdrant.Vectors {
	if p.Vectors == nil {
		return nil
	}
	if v := p.Vectors.GetVector(); v != nil {
		return &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vector{Vector: convertVector(v)}}
	}
	if vs := p.Vectors.GetVectors(); vs != nil {
		named := make(map[string]*qdrant.Vector, len(vs.GetVectors()))
		for k, v := range vs.GetVectors() {
			if cv := convertVector(v); cv != nil {
				named[k] = cv
			}
		}
		return qdrant.NewVectorsMap(named)
	}
	return nil
}

func (r *MigrateFromQdrantCmd) targetShardKey(point *qdrant.RetrievedPoint) (*qdrant.ShardKey, error) {
	// Qdrant Serverless has no shard keys, so the points are written without one.
	if r.Target.Serverless {
		return nil, nil
	}
	if r.ShardKey != "" {
		return qdrant.NewShardKeyKeyword(r.ShardKey), nil
	}
	if r.ShardKeyField == "" {
		return point.GetShardKey(), nil
	}

	value := point.GetPayload()[r.ShardKeyField]
	if value == nil {
		return nil, fmt.Errorf("payload field %q is missing", r.ShardKeyField)
	}
	if keyword, ok := value.GetKind().(*qdrant.Value_StringValue); ok && keyword.StringValue != "" {
		return qdrant.NewShardKeyKeyword(keyword.StringValue), nil
	}
	if number, ok := value.GetKind().(*qdrant.Value_IntegerValue); ok && number.IntegerValue >= 0 {
		return qdrant.NewShardKeyNum(uint64(number.IntegerValue)), nil
	}
	return nil, fmt.Errorf("payload field %q must be a non-empty string or non-negative integer", r.ShardKeyField)
}

// samplePointIDs fetches a random sample of point IDs from the source collection.
// These IDs are used as boundaries to divide the migration work among parallel workers.
func (r *MigrateFromQdrantCmd) samplePointIDs(ctx context.Context, client qdrantPoints, collection string, pointCount uint64) ([]*qdrant.PointId, error) {
	// TODO(Anush008): Check if there's a more optimal value for SAMPLE_SIZE_PER_WORKER.
	sampleSize := r.NumWorkers * SAMPLE_SIZE_PER_WORKER
	// Don't sample more points than are available in the collection.
	if uint64(sampleSize) > pointCount {
		sampleSize = int(pointCount)
	}
	if r.Source.Serverless {
		sampleSize = min(sampleSize, serverlessMaxLimit)
	}

	points, err := client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQuerySample(qdrant.Sample_Random),
		Limit:          qdrant.PtrOf(uint64(sampleSize)),
		WithPayload:    qdrant.NewWithPayload(false),
		WithVectors:    qdrant.NewWithVectors(false),
	})
	if err != nil {
		return nil, err
	}

	// The ranges will look like: (nil, A], (A, B], (B, C], (C, nil).
	// We sort the sampled IDs to create these ranges.
	ids := make([]*qdrant.PointId, len(points))
	for i, p := range points {
		ids[i] = p.Id
	}
	// Sort IDs to establish ordered boundaries for ranges.
	sort.Slice(ids, func(i, j int) bool { return comparePointIDs(ids[i], ids[j]) })
	return ids, nil
}

// processBatch handles the upserting of a batch of points to the target collection.
// It preserves source sharding or applies the configured target shard key.
func (r *MigrateFromQdrantCmd) processBatch(ctx context.Context, points []*qdrant.RetrievedPoint, targetClient *qdrantConnection, targetCollection string, shardKeys *sync.Map, wait bool) error {
	// Group points by their shard key.
	byShardKey := make(map[string][]*qdrant.PointStruct)
	shardKeyObjs := make(map[string]*qdrant.ShardKey)

	for _, p := range points {
		shardKey, err := r.targetShardKey(p)
		if err != nil {
			return fmt.Errorf("cannot route point %v: %w", p.GetId(), err)
		}
		key := ""
		if shardKey != nil {
			key = shardKey.String()
			shardKeyObjs[key] = shardKey
		}
		byShardKey[key] = append(byShardKey[key], &qdrant.PointStruct{
			Id:      p.Id,
			Payload: p.Payload,
			Vectors: convertVectors(p),
		})
	}

	// Upsert points for each shard key.
	for key, pts := range byShardKey {
		req := &qdrant.UpsertPoints{
			CollectionName: targetCollection,
			Points:         pts,
			Wait:           qdrant.PtrOf(wait),
		}
		if key != "" {
			if _, ok := shardKeys.Load(key); !ok {
				err := targetClient.cluster.CreateShardKey(ctx, targetCollection, &qdrant.CreateShardKey{ShardKey: shardKeyObjs[key]})
				if err != nil && !strings.Contains(err.Error(), "already exists") {
					return fmt.Errorf("failed to create shard key %s: %w", key, err)
				}
				shardKeys.Store(key, true)
			}
			req.ShardKeySelector = &qdrant.ShardKeySelector{ShardKeys: []*qdrant.ShardKey{shardKeyObjs[key]}}
		}
		if err := upsertWithRetry(ctx, targetClient, req); err != nil {
			return err
		}
	}
	return nil
}

// migrateData chooses between sequential and parallel migration based on the number of workers.
func (r *MigrateFromQdrantCmd) migrateData(ctx context.Context, sourceClient *qdrantConnection, sourceCollection string, targetClient *qdrantConnection, targetCollection string, sourcePointCount uint64) error {
	// An empty source has no points to sample for the parallel ranges.
	if r.NumWorkers > 1 && sourcePointCount > 0 {
		return r.migrateDataParallel(ctx, sourceClient, sourceCollection, targetClient, targetCollection, sourcePointCount)
	}
	return r.migrateDataSequential(ctx, sourceClient, sourceCollection, targetClient, targetCollection, sourcePointCount)
}

// migrateDataSequential performs the migration using a single worker.
func (r *MigrateFromQdrantCmd) migrateDataSequential(ctx context.Context, sourceClient *qdrantConnection, sourceCollection string, targetClient *qdrantConnection, targetCollection string, sourcePointCount uint64) error {
	var offset *qdrant.PointId
	var count uint64

	// If not restarting, get the last known offset to resume migration.
	if !r.Migration.Restart {
		var err error
		offset, count, err = commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceCollection)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, count)
	shardKeys := &sync.Map{}
	for {
		// Scroll through points from the source collection in batches.
		resp, err := sourceClient.GetPointsClient().Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: sourceCollection,
			Offset:         offset,
			Limit:          qdrant.PtrOf(uint32(r.Migration.BatchSize)),
			WithPayload:    qdrant.NewWithPayload(true),
			WithVectors:    qdrant.NewWithVectors(true),
		})
		if err != nil {
			return fmt.Errorf("failed to scroll data from source: %w", err)
		}

		points := resp.GetResult()
		if err := r.processBatch(ctx, points, targetClient, targetCollection, shardKeys, true); err != nil {
			return err
		}

		count += uint64(len(points))
		bar.Add(len(points))
		offset = resp.GetNextPageOffset()

		if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceCollection, offset, count); err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}
		if offset == nil {
			break
		}

		// Apply batch delay if configured (helps with rate limiting)
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// migrateDataParallel performs the migration using multiple workers in parallel.
func (r *MigrateFromQdrantCmd) migrateDataParallel(ctx context.Context, sourceClient *qdrantConnection, sourceCollection string, targetClient *qdrantConnection, targetCollection string, sourcePointCount uint64) error {
	pterm.Info.Printfln("Using parallel migration with %d workers", r.NumWorkers)

	// Resolve the range boundaries. On resume, the boundaries persisted by the previous run are reused.
	// Re-sampling would produce different ranges and make the stored per-range offsets point at the wrong places.
	ids, err := resolveRangeBoundaries(ctx, targetClient, r.Migration,
		fmt.Sprintf("%s-workers-%d-boundaries", sourceCollection, r.NumWorkers),
		func() ([]*qdrant.PointId, error) {
			return r.samplePointIDs(ctx, sourceClient, sourceCollection, sourcePointCount)
		})
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return fmt.Errorf("could not sample any point IDs from source collection %q", sourceCollection)
	}
	fingerprint, err := commons.BoundariesFingerprint(ids)
	if err != nil {
		return err
	}
	// The offset keys include NumWorkers and the boundaries fingerprint, so changing num-workers
	// (or the boundaries) starts a fresh migration instead of reusing incompatible offsets.
	offsetKeyFor := func(rangeID int) string {
		return fmt.Sprintf("%s-workers-%d-%s-range-%d", sourceCollection, r.NumWorkers, fingerprint, rangeID)
	}

	// Create the ranges based on the sorted sampled IDs.
	ranges := make([]rangeSpec, len(ids)+1)
	for i, id := range ids {
		ranges[i] = rangeSpec{id: i, end: id, offsetKey: offsetKeyFor(i)}
		if i > 0 {
			ranges[i].start = ids[i-1]
		}
	}
	ranges[len(ids)] = rangeSpec{id: len(ids), start: ids[len(ids)-1], offsetKey: offsetKeyFor(len(ids))}

	var totalProcessed uint64
	// If not restarting, load the progress for each range.
	if !r.Migration.Restart {
		for i := range ranges {
			offsetID, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, ranges[i].offsetKey)
			if err != nil {
				return fmt.Errorf("failed to get start offset: %w", err)
			}
			if offsetID != nil {
				ranges[i].start = offsetID
				totalProcessed += count
				pterm.Info.Printfln("Resuming range %d from offset (already processed %d points)", ranges[i].id, count)
			}
		}
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, totalProcessed)

	// Use a semaphore to limit the number of concurrent workers.
	shardKeys := &sync.Map{}
	errs := make(chan error, len(ranges))
	sem := make(chan struct{}, r.NumWorkers)

	// Start a goroutine for each range.
	for _, rg := range ranges {
		sem <- struct{}{}
		go func(rg rangeSpec) {
			errs <- r.migrateRange(ctx, sourceCollection, targetCollection, sourceClient, targetClient, rg, shardKeys, bar)
			<-sem
		}(rg)
	}

	// Wait for all workers to finish and collect any errors.
	var firstErr error
	for range ranges {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// migrateRange is the function executed by each worker in parallel migration.
// It scrolls through a specific range of points and upserts them to the target.
func (r *MigrateFromQdrantCmd) migrateRange(ctx context.Context, sourceCollection, targetCollection string, sourceClient, targetClient *qdrantConnection, rg rangeSpec, shardKeys *sync.Map, bar *pterm.ProgressbarPrinter) error {
	offsetKey := rg.offsetKey
	offset := rg.start
	var count uint64

	for { // Loop to scroll through the assigned range.
		resp, err := sourceClient.GetPointsClient().Scroll(ctx, &qdrant.ScrollPoints{
			CollectionName: sourceCollection,
			Offset:         offset,
			Limit:          qdrant.PtrOf(uint32(r.Migration.BatchSize)),
			WithPayload:    qdrant.NewWithPayload(true),
			WithVectors:    qdrant.NewWithVectors(true),
		})
		if err != nil {
			return err
		}

		points := resp.GetResult()
		if len(points) == 0 {
			break
		}

		// If this range has an end, truncate the batch to not go past the end ID.
		if rg.end != nil {
			for i, p := range points {
				if comparePointIDs(rg.end, p.Id) {
					points = points[:i]
					break
				}
			}
			if len(points) == 0 {
				break
			}
		}

		if err := r.processBatch(ctx, points, targetClient, targetCollection, shardKeys, false); err != nil {
			return err
		}

		count += uint64(len(points))
		bar.Add(len(points))

		// Store the progress for this range.
		if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey, points[len(points)-1].Id, count); err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		offset = resp.GetNextPageOffset()
		// Stop if we've reached the end of the collection or the end of the assigned range.
		if offset == nil || (rg.end != nil && !comparePointIDs(points[len(points)-1].Id, rg.end)) {
			break
		}

		// Apply batch delay if configured (helps with rate limiting)
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}
	return nil
}
