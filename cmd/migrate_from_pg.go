package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromPGCmd struct {
	PG             commons.PGConfig        `embed:"" prefix:"pg."`
	Qdrant         commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration      commons.MigrationConfig `embed:"" prefix:"migration."`
	DistanceMetric map[string]string       `prefix:"qdrant." help:"Map of vector field names to distance metrics (cosine,dot,euclid,manhattan). Default is cosine if not specified."`
	NumWorkers     int                     `help:"Number of parallel workers for data migration (0 = number of CPU cores)" default:"0" prefix:"migration."`

	targetHost string
	targetPort int
	targetTLS  bool
}

// pgRangeSpec defines a key range for a worker to process in parallel migration.
type pgRangeSpec struct {
	id       int
	startKey *string
	endKey   *string
}

func (r *MigrateFromPGCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromPGCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromPGCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Postgres to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	if r.NumWorkers == 0 {
		r.NumWorkers = runtime.NumCPU()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceConn, err := r.connectToPG(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Postgres source: %w", err)
	}
	defer sourceConn.Close(ctx)

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countPGRows(ctx, sourceConn)
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceConn, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("postgres", r.PG.Table, r.Qdrant.Collection)

	err = r.migrateData(ctx, sourceConn, targetClient, sourcePointCount)
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

func (r *MigrateFromPGCmd) connectToPG(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, r.PG.Url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Postgres: %w", err)
	}

	err = pgxvec.RegisterTypes(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to register pgvector types: %w", err)
	}

	return conn, nil
}

func (r *MigrateFromPGCmd) countPGRows(ctx context.Context, conn *pgx.Conn) (uint64, error) {
	tableIdent := pgx.Identifier{r.PG.Table}.Sanitize()
	row := conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableIdent))

	var count int64
	err := row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	return uint64(count), nil
}

func getVectorColumns(ctx context.Context, conn *pgx.Conn, table string) (map[string]uint64, error) {
	tableIdent := pgx.Identifier{table}.Sanitize()
	query := `
	SELECT
		attname AS column_name,
		atttypmod AS dimensions
	FROM
		pg_attribute
	WHERE
		attrelid = $1::regclass
		AND attnum > 0
		AND NOT attisdropped
		AND format_type(atttypid, atttypmod) LIKE 'vector%';
	`
	rows, err := conn.Query(ctx, query, tableIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to query vector columns: %w", err)
	}
	defer rows.Close()

	vectorMap := make(map[string]uint64)
	for rows.Next() {
		var col string
		var dim int32
		err := rows.Scan(&col, &dim)
		if err != nil {
			return nil, fmt.Errorf("failed to scan vector column: %w", err)
		}
		if dim <= 0 {
			return nil, fmt.Errorf("invalid dimension for column %s", col)
		}
		vectorMap[col] = uint64(dim)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading vector columns: %w", err)
	}
	return vectorMap, nil
}

func (r *MigrateFromPGCmd) prepareTargetCollection(ctx context.Context, sourceConn *pgx.Conn, targetClient *qdrant.Client) error {
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

	vectorDims, err := getVectorColumns(ctx, sourceConn, r.PG.Table)
	if err != nil {
		return fmt.Errorf("failed to get vector columns: %w", err)
	}

	distanceMapping := map[string]qdrant.Distance{
		"euclid":    qdrant.Distance_Euclid,
		"cosine":    qdrant.Distance_Cosine,
		"dot":       qdrant.Distance_Dot,
		"manhattan": qdrant.Distance_Manhattan,
	}

	vectorParamsMap := make(map[string]*qdrant.VectorParams)
	for field, dimension := range vectorDims {
		distanceMetric := "cosine"
		if specifiedDistance, ok := r.DistanceMetric[field]; ok {
			distanceMetric = specifiedDistance
		}
		if _, valid := distanceMapping[distanceMetric]; !valid {
			return fmt.Errorf("invalid distance metric '%s' for vector '%s'", distanceMetric, field)
		}

		vectorParamsMap[field] = &qdrant.VectorParams{
			Size:     dimension,
			Distance: distanceMapping[distanceMetric],
		}
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

func (r *MigrateFromPGCmd) migrateData(ctx context.Context, sourceConn *pgx.Conn, targetClient *qdrant.Client, sourcePointCount uint64) error {
	if r.NumWorkers > 1 {
		return r.migrateDataParallel(ctx, sourceConn, targetClient, sourcePointCount)
	}
	return r.migrateDataSequential(ctx, sourceConn, targetClient, sourcePointCount)
}

// migrateDataSequential performs the migration using a single worker.
func (r *MigrateFromPGCmd) migrateDataSequential(ctx context.Context, sourceConn *pgx.Conn, targetClient *qdrant.Client, sourcePointCount uint64) error {
	batchSize := r.Migration.BatchSize

	offsetCount := uint64(0)

	if !r.Migration.Restart {
		_, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.PG.Table)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	for {
		var selectColumns string
		if len(r.PG.Columns) > 0 {
			var quotedCols []string
			for _, col := range r.PG.Columns {
				quotedCols = append(quotedCols, pgx.Identifier{col}.Sanitize())
			}
			selectColumns = strings.Join(quotedCols, ", ")
		} else {
			selectColumns = "*"
		}
		tableIdent := pgx.Identifier{r.PG.Table}.Sanitize()
		keyColIdent := pgx.Identifier{r.PG.KeyColumn}.Sanitize()
		query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT $1 OFFSET $2", selectColumns, tableIdent, keyColIdent)
		rows, err := sourceConn.Query(ctx, query, batchSize, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to query PG: %w", err)
		}

		batchRows, err := pgx.CollectRows(rows, pgx.RowToMap)
		if err != nil {
			return fmt.Errorf("failed to collect rows: %w", err)
		}

		if len(batchRows) == 0 {
			break
		}

		targetPoints := r.convertRowsToPoints(batchRows)

		_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return fmt.Errorf("failed to insert data into target: %w", err)
		}

		// Just a placeholder ID.
		// We're only using the offset count
		offsetID := qdrant.NewIDNum(0)
		offsetCount += uint64(len(targetPoints))
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.PG.Table, offsetID, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))
	}

	pterm.Success.Printfln("Data migration finished successfully")

	return nil
}

// sampleKeyValues samples random key values from the table to create range boundaries.
// It returns keys as text but sorted in the native column order.
func (r *MigrateFromPGCmd) sampleKeyValues(ctx context.Context, conn *pgx.Conn, sampleSize int) ([]string, error) {
	tableIdent := pgx.Identifier{r.PG.Table}.Sanitize()
	keyColIdent := pgx.Identifier{r.PG.KeyColumn}.Sanitize()

	// Sample random keys, but return them sorted by their native column type order.
	query := fmt.Sprintf(
		"SELECT %s::text FROM (SELECT %s FROM %s ORDER BY RANDOM() LIMIT $1) sub ORDER BY %s",
		keyColIdent, keyColIdent, tableIdent, keyColIdent,
	)
	rows, err := conn.Query(ctx, query, sampleSize)
	if err != nil {
		return nil, fmt.Errorf("failed to sample keys: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("failed to scan key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading keys: %w", err)
	}

	return keys, nil
}

// createRangesFromSamples divides the sorted sampled keys into ranges for parallel workers.
// Each boundary key becomes the endKey of one range and startKey of the next, creating
// non-overlapping ranges: [nil, k1), [k1, k2), ..., [kN, nil).
func (r *MigrateFromPGCmd) createRangesFromSamples(keys []string) []pgRangeSpec {
	if len(keys) == 0 {
		return []pgRangeSpec{{id: 0}}
	}

	numRanges := r.NumWorkers
	if len(keys) < numRanges {
		numRanges = len(keys) + 1
	}

	ranges := make([]pgRangeSpec, numRanges)
	for i := range ranges {
		ranges[i].id = i
	}

	for i := 1; i < numRanges; i++ {
		idx := (i * len(keys)) / numRanges
		ranges[i-1].endKey = &keys[idx]
		ranges[i].startKey = &keys[idx]
	}

	return ranges
}

// migrateDataParallel performs the migration using multiple workers in parallel.
func (r *MigrateFromPGCmd) migrateDataParallel(ctx context.Context, sourceConn *pgx.Conn, targetClient *qdrant.Client, sourcePointCount uint64) error {
	pterm.Info.Printfln("Using parallel migration with %d workers", r.NumWorkers)

	if sourcePointCount == 0 {
		pterm.Info.Println("Table is empty, nothing to migrate")
		return nil
	}

	sampleSize := r.NumWorkers * SAMPLE_SIZE_PER_WORKER
	if uint64(sampleSize) > sourcePointCount {
		sampleSize = int(sourcePointCount)
	}

	keys, err := r.sampleKeyValues(ctx, sourceConn, sampleSize)
	if err != nil {
		return fmt.Errorf("failed to sample keys: %w", err)
	}

	ranges := r.createRangesFromSamples(keys)

	poolConfig, err := pgxpool.ParseConfig(r.PG.Url)
	if err != nil {
		return fmt.Errorf("failed to parse connection string for pool: %w", err)
	}
	poolConfig.MaxConns = int32(r.NumWorkers)
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}
	defer pool.Close()

	// If not restarting, load the progress for each range.
	// The offset key includes NumWorkers, so changing num-workers will start a fresh migration.
	var totalProcessed uint64
	if !r.Migration.Restart {
		for i := range ranges {
			offsetKey := fmt.Sprintf("%s-workers-%d-range-%d", r.PG.Table, r.NumWorkers, ranges[i].id)
			_, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey)
			if err != nil {
				return fmt.Errorf("failed to get start offset: %w", err)
			}
			totalProcessed += count
			if count > 0 {
				pterm.Info.Printfln("Resuming range %d (already processed %d points)", ranges[i].id, count)
			}
		}
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, totalProcessed)

	// Use a semaphore to limit the number of concurrent workers.
	errs := make(chan error, len(ranges))
	sem := make(chan struct{}, r.NumWorkers)

	// Start a goroutine for each range.
	for _, rg := range ranges {
		sem <- struct{}{}
		go func(rg pgRangeSpec) {
			errs <- r.migrateRange(ctx, pool, targetClient, rg, bar)
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
// It queries a specific key range and upserts the rows to the target.
func (r *MigrateFromPGCmd) migrateRange(ctx context.Context, pool *pgxpool.Pool, targetClient *qdrant.Client, rg pgRangeSpec, bar *pterm.ProgressbarPrinter) error {
	offsetKey := fmt.Sprintf("%s-workers-%d-range-%d", r.PG.Table, r.NumWorkers, rg.id)
	batchSize := r.Migration.BatchSize

	var lastKey *string
	var count uint64
	if !r.Migration.Restart {
		offsetID, c, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		if offsetID != nil {
			if uuid := offsetID.GetUuid(); uuid != "" {
				lastKey = &uuid
			}
		}
		count = c
	}

	tableIdent := pgx.Identifier{r.PG.Table}.Sanitize()
	keyColIdent := pgx.Identifier{r.PG.KeyColumn}.Sanitize()

	var selectColumns string
	if len(r.PG.Columns) > 0 {
		var quotedCols []string
		for _, col := range r.PG.Columns {
			quotedCols = append(quotedCols, pgx.Identifier{col}.Sanitize())
		}
		selectColumns = strings.Join(quotedCols, ", ")
	} else {
		selectColumns = "*"
	}

	for {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("failed to acquire connection: %w", err)
		}

		effectiveStart := rg.startKey
		if lastKey != nil {
			effectiveStart = lastKey
		}

		var whereClauses []string
		var args []interface{}

		// Use native column type comparison
		// PostgreSQL will implicitly cast the text parameter to the column's actual type.
		if effectiveStart != nil {
			args = append(args, *effectiveStart)
			whereClauses = append(whereClauses, fmt.Sprintf("%s > $%d", keyColIdent, len(args)))
		}
		if rg.endKey != nil {
			args = append(args, *rg.endKey)
			whereClauses = append(whereClauses, fmt.Sprintf("%s <= $%d", keyColIdent, len(args)))
		}

		args = append(args, batchSize)
		whereClause := ""
		if len(whereClauses) > 0 {
			whereClause = "WHERE " + strings.Join(whereClauses, " AND ") + " "
		}

		query := fmt.Sprintf("SELECT %s FROM %s %sORDER BY %s LIMIT $%d",
			selectColumns, tableIdent, whereClause, keyColIdent, len(args))

		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			conn.Release()
			return fmt.Errorf("failed to query PG: %w", err)
		}

		batchRows, err := pgx.CollectRows(rows, pgx.RowToMap)
		conn.Release()
		if err != nil {
			return fmt.Errorf("failed to collect rows: %w", err)
		}

		if len(batchRows) == 0 {
			break
		}

		targetPoints := r.convertRowsToPoints(batchRows)

		// Upsert with retries to handle Qdrant's transient consistency errors during parallel writes.
		var upsertErr error
		for attempt := 0; attempt < MAX_RETRIES; attempt++ {
			_, upsertErr = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
				CollectionName: r.Qdrant.Collection,
				Points:         targetPoints,
				Wait:           qdrant.PtrOf(false),
			})
			if upsertErr == nil || !strings.Contains(upsertErr.Error(), "Please retry") {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
		if upsertErr != nil {
			return fmt.Errorf("failed to insert data into target: %w", upsertErr)
		}

		lastRow := batchRows[len(batchRows)-1]
		if keyVal, ok := lastRow[r.PG.KeyColumn]; ok {
			keyStr := keyToString(keyVal)
			lastKey = &keyStr
		}

		count += uint64(len(targetPoints))
		bar.Add(len(targetPoints))

		if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey, qdrant.NewIDUUID(*lastKey), count); err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}
	}

	return nil
}

// convertRowsToPoints converts PostgreSQL rows to Qdrant points.
func (r *MigrateFromPGCmd) convertRowsToPoints(batchRows []map[string]interface{}) []*qdrant.PointStruct {
	var targetPoints []*qdrant.PointStruct
	for _, row := range batchRows {
		point := &qdrant.PointStruct{}
		vectors := make(map[string]*qdrant.Vector)
		payload := make(map[string]interface{})

		for col, val := range row {
			if col == r.PG.KeyColumn {
				idStr := keyToString(val)
				point.Id = arbitraryIDToUUID(idStr)
			}

			switch v := val.(type) {
			case pgvector.Vector:
				vectors[col] = qdrant.NewVectorDense(v.Slice())
			default:
				payload[col] = sanitizeValue(val)
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
	return targetPoints
}

func keyToString(val any) string {
	if s, ok := tryFormatAsUUID(val); ok {
		return s
	}
	if b, ok := val.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(val)
}

func tryFormatAsUUID(val any) (string, bool) {
	switch v := val.(type) {
	case [16]byte:
		return uuid.UUID(v).String(), true
	case []byte:
		if len(v) == 16 {
			return uuid.UUID(v).String(), true
		}
	}
	return "", false
}

// Recursively converts value unsupported as payload in Qdrant to string.
// Otherwise, it returns the value as is.
func sanitizeValue(val any) any {
	if s, ok := tryFormatAsUUID(val); ok {
		return s
	}

	switch v := val.(type) {
	// Types supported by qdrant.NewValueMap()
	// https://github.com/qdrant/go-client/blob/cf8426be6063135411fe063e062cfac5b57c2ceb/qdrant/value_map.go#L29-L44
	case nil, bool, int, int32, int64, uint, uint32, uint64, float32, float64, string, []byte:
		return v
	case time.Time:
		return v.Format(time.RFC3339)
	case []interface{}:
		newArr := make([]interface{}, len(v))
		for i, elem := range v {
			newArr[i] = sanitizeValue(elem)
		}
		return newArr
	case map[string]interface{}:
		newMap := make(map[string]interface{}, len(v))
		for k, elem := range v {
			newMap[k] = sanitizeValue(elem)
		}
		return newMap
	default:
		return fmt.Sprint(v)
	}
}
