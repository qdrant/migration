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

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

const (
	// MAX_RETRIES is the maximum number of retries for upsert operations on transient errors.
	MAX_RETRIES = 3
	// SAMPLE_SIZE_PER_WORKER is the number of points to sample per worker to determine ranges for parallel migration.
	SAMPLE_SIZE_PER_WORKER = 10
)

type MigrateFromQdrantCmd struct {
	Source               commons.QdrantConfig    `embed:"" prefix:"source."`
	Target               commons.QdrantConfig    `embed:"" prefix:"target."`
	Migration            commons.MigrationConfig `embed:"" prefix:"migration."`
	MaxMessageSize       int                     `help:"Maximum gRPC message size in bytes (default: 33554432 = 32MB)" default:"33554432" prefix:"source."`
	EnsurePayloadIndexes bool                    `help:"Ensure payload indexes from the source are created on the target" default:"true" prefix:"target."`
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
	return nil
}

func (r *MigrateFromQdrantCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
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

	sourceClient, err := connectToQdrant(globals, r.sourceHost, r.sourcePort, r.Source.APIKey, r.sourceTLS, r.MaxMessageSize)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceClient.Close()

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Target.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetClient.Close()

	if err := commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient); err != nil {
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

	displayMigrationStart("qdrant", r.Source.Collection, r.Target.Collection)

	if err := r.migrateData(ctx, sourceClient, r.Source.Collection, targetClient, r.Target.Collection, sourcePointCount); err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Target.Collection,
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

// prepareTargetCollection ensures the target collection exists and is configured correctly.
// It can create the collection based on the source's schema and create payload indexes.
func (r *MigrateFromQdrantCmd) prepareTargetCollection(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string) error {
	sourceCollectionInfo, err := sourceClient.GetCollectionInfo(ctx, sourceCollection)
	if err != nil {
		return fmt.Errorf("failed to get source collection info: %w", err)
	}

	if r.Migration.CreateCollection {
		exists, err := targetClient.CollectionExists(ctx, targetCollection)
		if err != nil {
			return fmt.Errorf("failed to check if collection exists: %w", err)
		}
		if exists {
			fmt.Print("\n")
			pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", targetCollection)
		} else {
			params := sourceCollectionInfo.Config.GetParams()
			if err := targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
				CollectionName:         targetCollection,
				HnswConfig:             sourceCollectionInfo.Config.GetHnswConfig(),
				WalConfig:              sourceCollectionInfo.Config.GetWalConfig(),
				OptimizersConfig:       sourceCollectionInfo.Config.GetOptimizerConfig(),
				ShardNumber:            &params.ShardNumber,
				OnDiskPayload:          &params.OnDiskPayload,
				VectorsConfig:          params.VectorsConfig,
				ReplicationFactor:      params.ReplicationFactor,
				WriteConsistencyFactor: params.WriteConsistencyFactor,
				QuantizationConfig:     sourceCollectionInfo.Config.GetQuantizationConfig(),
				ShardingMethod:         params.ShardingMethod,
				SparseVectorsConfig:    params.SparseVectorsConfig,
				StrictModeConfig:       sourceCollectionInfo.Config.GetStrictModeConfig(),
			}); err != nil {
				return fmt.Errorf("failed to create target collection: %w", err)
			}
		}
	}

	targetCollectionInfo, err := targetClient.GetCollectionInfo(ctx, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get target collection information: %w", err)
	}

	// If EnsurePayloadIndexes is enabled, create any missing payload indexes on the target.
	if r.EnsurePayloadIndexes {
		for name, schemaInfo := range sourceCollectionInfo.GetPayloadSchema() {
			fieldType := getFieldType(schemaInfo.GetDataType())
			if fieldType == nil {
				continue
			}

			// If there is already an index in the target collection, skip.
			if _, ok := targetCollectionInfo.GetPayloadSchema()[name]; ok {
				continue
			}

			if _, err := targetClient.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
				CollectionName:   r.Target.Collection,
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
func convertVector(v *qdrant.VectorOutput) *qdrant.Vector {
	if v == nil {
		return nil
	}
	return &qdrant.Vector{Data: v.GetData(), Indices: v.GetIndices(), VectorsCount: v.VectorsCount}
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
			named[k] = convertVector(v)
		}
		return &qdrant.Vectors{VectorsOptions: &qdrant.Vectors_Vectors{Vectors: &qdrant.NamedVectors{Vectors: named}}}
	}
	return nil
}

// samplePointIDs fetches a random sample of point IDs from the source collection.
// These IDs are used as boundaries to divide the migration work among parallel workers.
func (r *MigrateFromQdrantCmd) samplePointIDs(ctx context.Context, client *qdrant.Client, collection string, pointCount uint64) ([]*qdrant.PointId, error) {
	// TODO(Anush008): Check if there's a more optimal value for SAMPLE_SIZE_PER_WORKER.
	sampleSize := r.NumWorkers * SAMPLE_SIZE_PER_WORKER
	// Don't sample more points than are available in the collection.
	if uint64(sampleSize) > pointCount {
		sampleSize = int(pointCount)
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
// It deals with sharding by creating shard keys if they don't exist and retries on transient errors.
func (r *MigrateFromQdrantCmd) processBatch(ctx context.Context, points []*qdrant.RetrievedPoint, targetClient *qdrant.Client, targetCollection string, shardKeys *sync.Map, wait bool) error {
	// Group points by their shard key.
	byShardKey := make(map[string][]*qdrant.PointStruct)
	shardKeyObjs := make(map[string]*qdrant.ShardKey)

	for _, p := range points {
		key := ""
		// Determine the shard key for the point.
		if sk := p.GetShardKey(); sk != nil {
			if kw := sk.GetKeyword(); kw != "" {
				key = kw // String-based shard key.
			} else {
				key = fmt.Sprintf("%d", sk.GetNumber())
			}
			shardKeyObjs[key] = sk
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
			// If the shard key is new, create it on the target collection.
			if _, ok := shardKeys.Load(key); !ok {
				err := targetClient.CreateShardKey(ctx, targetCollection, &qdrant.CreateShardKey{ShardKey: shardKeyObjs[key]})
				if err != nil && !strings.Contains(err.Error(), "already exists") {
					return fmt.Errorf("failed to create shard key %s: %w", key, err)
				}
				shardKeys.Store(key, true)
			}
			// Specify the shard key for the upsert request.
			req.ShardKeySelector = &qdrant.ShardKeySelector{ShardKeys: []*qdrant.ShardKey{shardKeyObjs[key]}}
		}
		var err error
		// Upsert with retries.
		// This is to handle Qdrant's transient consistency errors during parallel writes.
		for attempt := 0; attempt < MAX_RETRIES; attempt++ {
			_, err = targetClient.Upsert(ctx, req)
			if err == nil || !strings.Contains(err.Error(), "Please retry") {
				break
			}
			// Exponential backoff for retries.
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
		if err != nil {
			return fmt.Errorf("failed to insert data into target: %w", err)
		}
	}
	return nil
}

// migrateData chooses between sequential and parallel migration based on the number of workers.
func (r *MigrateFromQdrantCmd) migrateData(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string, sourcePointCount uint64) error {
	if r.NumWorkers > 1 {
		return r.migrateDataParallel(ctx, sourceClient, sourceCollection, targetClient, targetCollection, sourcePointCount)
	}
	return r.migrateDataSequential(ctx, sourceClient, sourceCollection, targetClient, targetCollection, sourcePointCount)
}

// migrateDataSequential performs the migration using a single worker.
func (r *MigrateFromQdrantCmd) migrateDataSequential(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string, sourcePointCount uint64) error {
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
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// migrateDataParallel performs the migration using multiple workers in parallel.
func (r *MigrateFromQdrantCmd) migrateDataParallel(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string, sourcePointCount uint64) error {
	pterm.Info.Printfln("Using parallel migration with %d workers", r.NumWorkers)

	// Sample points to define ranges for parallel workers.
	ids, err := r.samplePointIDs(ctx, sourceClient, sourceCollection, sourcePointCount)
	if err != nil {
		return err
	}

	// Create the ranges based on the sorted sampled IDs.
	ranges := make([]rangeSpec, len(ids)+1)
	for i, id := range ids {
		ranges[i] = rangeSpec{id: i, end: id}
		if i > 0 {
			ranges[i].start = ids[i-1]
		}
	}
	ranges[len(ids)] = rangeSpec{id: len(ids), start: ids[len(ids)-1]}

	var totalProcessed uint64
	// If not restarting, load the progress for each range.
	// The offset key includes NumWorkers, so changing num-workers will start a fresh migration.
	if !r.Migration.Restart {
		for i := range ranges {
			offsetKey := fmt.Sprintf("%s-workers-%d-range-%d", sourceCollection, r.NumWorkers, ranges[i].id)
			offsetID, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey)
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
func (r *MigrateFromQdrantCmd) migrateRange(ctx context.Context, sourceCollection, targetCollection string, sourceClient, targetClient *qdrant.Client, rg rangeSpec, shardKeys *sync.Map, bar *pterm.ProgressbarPrinter) error {
	offsetKey := fmt.Sprintf("%s-workers-%d-range-%d", sourceCollection, r.NumWorkers, rg.id)
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
	}
	return nil
}
