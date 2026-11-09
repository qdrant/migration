//go:build !no_clickhouse

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

func init() {
	registerCommand[MigrateFromClickHouseCmd]("clickhouse", "Migrate data from a ClickHouse database to Qdrant.")
}

type MigrateFromClickHouseCmd struct {
	ClickHouse     commons.ClickHouseConfig `embed:"" prefix:"clickhouse."`
	Qdrant         commons.QdrantConfig     `embed:"" prefix:"qdrant."`
	Migration      commons.MigrationConfig  `embed:"" prefix:"migration."`
	DistanceMetric map[string]string        `prefix:"qdrant." help:"Map of vector field names to distance metrics (cosine,dot,euclid,manhattan). Default is cosine if not specified."`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromClickHouseCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromClickHouseCmd) Validate() error {
	if err := validateBatchSize(r.Migration.BatchSize); err != nil {
		return err
	}
	if len(r.ClickHouse.VectorColumns) == 0 {
		return fmt.Errorf("at least one vector column must be specified via --clickhouse.vector-columns")
	}

	// If an explicit column projection is given, it must contain the key column and
	// every vector column; otherwise points would silently be stored without vectors.
	if len(r.ClickHouse.Columns) > 0 {
		selected := make(map[string]bool, len(r.ClickHouse.Columns))
		for _, col := range r.ClickHouse.Columns {
			selected[col] = true
		}
		if !selected[r.ClickHouse.KeyColumn] {
			return fmt.Errorf("--clickhouse.columns must include the key column %q", r.ClickHouse.KeyColumn)
		}
		for _, vc := range r.ClickHouse.VectorColumns {
			if !selected[vc] {
				return fmt.Errorf("--clickhouse.columns must include the vector column %q", vc)
			}
		}
	}

	return nil
}

func (r *MigrateFromClickHouseCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("ClickHouse to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceConn, err := r.connectToClickHouse(ctx, globals)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse source: %w", err)
	}
	defer sourceConn.Close()

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := r.countClickHouseRows(ctx, sourceConn)
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceConn, targetClient)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("clickhouse", r.ClickHouse.Table, r.Qdrant.Collection)

	err = r.migrateData(ctx, sourceConn, targetClient, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	if err := verifyTargetCount(ctx, targetClient, r.Qdrant.Collection, sourcePointCount, r.Migration.OffsetsCollection); err != nil {
		return err
	}

	err = commons.DeleteOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}

	return nil
}

func (r *MigrateFromClickHouseCmd) connectToClickHouse(ctx context.Context, globals *Globals) (driver.Conn, error) {
	options := &clickhouse.Options{
		Addr: []string{r.ClickHouse.Addr},
		Auth: clickhouse.Auth{
			Database: r.ClickHouse.Database,
			Username: r.ClickHouse.Username,
			Password: r.ClickHouse.Password,
		},
	}

	if r.ClickHouse.Secure {
		options.TLS = &tls.Config{
			InsecureSkipVerify: globals.SkipTlsVerification,
		}
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, nil
}

// quoteClickHouseIdent quotes an identifier using backticks, escaping any embedded backticks.
func quoteClickHouseIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// tableIdentifier returns the fully qualified, quoted `database`.`table` identifier.
func (r *MigrateFromClickHouseCmd) tableIdentifier() string {
	if r.ClickHouse.Database == "" {
		return quoteClickHouseIdent(r.ClickHouse.Table)
	}
	return quoteClickHouseIdent(r.ClickHouse.Database) + "." + quoteClickHouseIdent(r.ClickHouse.Table)
}

func (r *MigrateFromClickHouseCmd) countClickHouseRows(ctx context.Context, conn driver.Conn) (uint64, error) {
	var count uint64
	query := fmt.Sprintf("SELECT count() FROM %s", r.tableIdentifier())
	if err := conn.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return count, nil
}

// keyColumnType returns the ClickHouse type name of the key column, used to CAST the
// resume cursor back to its native type during keyset pagination.
func (r *MigrateFromClickHouseCmd) keyColumnType(ctx context.Context, conn driver.Conn) (string, error) {
	var typeName string
	query := fmt.Sprintf(
		"SELECT toTypeName(%s) FROM %s LIMIT 1",
		quoteClickHouseIdent(r.ClickHouse.KeyColumn), r.tableIdentifier(),
	)
	if err := conn.QueryRow(ctx, query).Scan(&typeName); err != nil {
		return "", fmt.Errorf("failed to determine type of key column %q: %w", r.ClickHouse.KeyColumn, err)
	}
	return typeName, nil
}

// selectColumns builds the comma separated list of quoted columns to select, or "*".
func (r *MigrateFromClickHouseCmd) selectColumns() string {
	if len(r.ClickHouse.Columns) == 0 {
		return "*"
	}
	quoted := make([]string, len(r.ClickHouse.Columns))
	for i, col := range r.ClickHouse.Columns {
		quoted[i] = quoteClickHouseIdent(col)
	}
	return strings.Join(quoted, ", ")
}

func (r *MigrateFromClickHouseCmd) vectorColumnSet() map[string]bool {
	set := make(map[string]bool, len(r.ClickHouse.VectorColumns))
	for _, col := range r.ClickHouse.VectorColumns {
		set[col] = true
	}
	return set
}

// getVectorDimensions samples one non-empty row per vector column to determine its dimensionality.
func (r *MigrateFromClickHouseCmd) getVectorDimensions(ctx context.Context, conn driver.Conn) (map[string]uint64, error) {
	dims := make(map[string]uint64, len(r.ClickHouse.VectorColumns))
	for _, col := range r.ClickHouse.VectorColumns {
		colIdent := quoteClickHouseIdent(col)
		query := fmt.Sprintf(
			"SELECT length(%s) FROM %s WHERE length(%s) > 0 LIMIT 1",
			colIdent, r.tableIdentifier(), colIdent,
		)
		var dim uint64
		if err := conn.QueryRow(ctx, query).Scan(&dim); err != nil {
			return nil, fmt.Errorf("failed to determine dimension for vector column %q (ensure it is a non-empty Array(Float32)): %w", col, err)
		}
		if dim == 0 {
			return nil, fmt.Errorf("vector column %q has zero dimension", col)
		}
		dims[col] = dim
	}
	return dims, nil
}

func (r *MigrateFromClickHouseCmd) prepareTargetCollection(ctx context.Context, conn driver.Conn, targetClient *qdrant.Client) error {
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

	vectorDims, err := r.getVectorDimensions(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to get vector dimensions: %w", err)
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

// migrateData streams rows using keyset pagination (WHERE key > lastKey ORDER BY key)
// and upserts them to Qdrant.
func (r *MigrateFromClickHouseCmd) migrateData(ctx context.Context, conn driver.Conn, targetClient *qdrant.Client, sourcePointCount uint64) error {
	if sourcePointCount == 0 {
		pterm.Info.Println("Source table is empty, nothing to migrate")
		return nil
	}

	batchSize := r.Migration.BatchSize
	scanned := uint64(0)
	lastKey := ""

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.ClickHouse.Table)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		scanned = count
		if id != nil {
			lastKey = id.GetUuid()
		}
	}

	keyType, err := r.keyColumnType(ctx, conn)
	if err != nil {
		return err
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, scanned)

	keyColIdent := quoteClickHouseIdent(r.ClickHouse.KeyColumn)
	selectColumns := r.selectColumns()

	for {
		var query string
		var args []any
		if lastKey == "" {
			query = fmt.Sprintf(
				"SELECT %s FROM %s ORDER BY %s ASC LIMIT ?",
				selectColumns, r.tableIdentifier(), keyColIdent,
			)
			args = []any{batchSize}
		} else {
			// CAST the stored cursor (always a string) back to the key's native type so the
			// comparison matches the ORDER BY collation and avoids a full re-sort each batch.
			query = fmt.Sprintf(
				"SELECT %s FROM %s WHERE %s > CAST(? AS %s) ORDER BY %s ASC LIMIT ?",
				selectColumns, r.tableIdentifier(), keyColIdent, keyType, keyColIdent,
			)
			args = []any{lastKey, batchSize}
		}

		batchRows, err := r.queryBatch(ctx, conn, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query ClickHouse: %w", err)
		}

		if len(batchRows) == 0 {
			break
		}

		// Advance the cursor to the last key in the batch so we never loop on the same key.
		lastRow := batchRows[len(batchRows)-1]
		nextKey, ok := lastRow[r.ClickHouse.KeyColumn]
		if !ok {
			return fmt.Errorf("key column %q not found in result set", r.ClickHouse.KeyColumn)
		}
		lastKey = clickhouseKeyToString(nextKey)

		targetPoints, err := r.convertRowsToPoints(batchRows)
		if err != nil {
			return err
		}

		if len(targetPoints) > 0 {
			err = upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
				CollectionName: r.Qdrant.Collection,
				Points:         targetPoints,
				Wait:           qdrant.PtrOf(true),
			})
			if err != nil {
				return err
			}
		}

		scanned += uint64(len(batchRows))
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.ClickHouse.Table, qdrant.NewIDUUID(lastKey), scanned)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(batchRows))

		// Apply batch delay if configured (helps with rate limiting).
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}

		// A short batch means we've reached the end of the table.
		if len(batchRows) < batchSize {
			break
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// queryBatch executes the query and dynamically scans each row into a map keyed by column name.
func (r *MigrateFromClickHouseCmd) queryBatch(ctx context.Context, conn driver.Conn, query string, args ...any) ([]map[string]any, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnNames := rows.Columns()
	columnTypes := rows.ColumnTypes()

	var batchRows []map[string]any
	for rows.Next() {
		scanDest := make([]any, len(columnTypes))
		for i, ct := range columnTypes {
			scanDest[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := rows.Scan(scanDest...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]any, len(columnNames))
		for i, name := range columnNames {
			row[name] = derefValue(scanDest[i])
		}
		batchRows = append(batchRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}

	return batchRows, nil
}

// convertRowsToPoints converts ClickHouse rows into Qdrant points. Consistent with the
// pgvector and Milvus sources, a row whose configured vector columns are all empty is still
// written as a point (with its payload and no named vectors) rather than being dropped.
func (r *MigrateFromClickHouseCmd) convertRowsToPoints(batchRows []map[string]any) ([]*qdrant.PointStruct, error) {
	vectorSet := r.vectorColumnSet()

	targetPoints := make([]*qdrant.PointStruct, 0, len(batchRows))
	for _, row := range batchRows {
		point := &qdrant.PointStruct{}
		vectors := make(map[string]*qdrant.Vector)
		payload := make(map[string]any)
		idSet := false

		for col, val := range row {
			if col == r.ClickHouse.KeyColumn {
				point.Id = arbitraryIDToUUID(clickhouseKeyToString(val))
				idSet = true
			}

			if vectorSet[col] {
				vec, err := toFloat32Slice(val)
				if err != nil {
					return nil, fmt.Errorf("failed to convert column %q to a vector: %w", col, err)
				}
				// An empty/missing vector is simply left out of the named-vector map; like
				// pgvector and Milvus the point is still written, just without that vector.
				if len(vec) == 0 {
					continue
				}
				vectors[col] = qdrant.NewVector(vec...)
			} else {
				payload[col] = clickhouseSanitizeValue(val)
			}
		}

		if !idSet {
			return nil, fmt.Errorf("key column %q not found in result set", r.ClickHouse.KeyColumn)
		}

		point.Vectors = qdrant.NewVectorsMap(vectors)
		point.Payload = qdrant.NewValueMap(payload)
		targetPoints = append(targetPoints, point)
	}

	return targetPoints, nil
}

// derefValue fully dereferences pointers (including Nullable columns scanned as pointers).
func derefValue(v any) any {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil
	}
	return rv.Interface()
}

// clickhouseKeyToString renders a key column value as a string for use as a Qdrant point ID
// and as the keyset-pagination cursor.
func clickhouseKeyToString(val any) string {
	switch v := val.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

// clickhouseSanitizeValue converts a ClickHouse-driver value into a type that
// qdrant.NewValueMap can represent. Unlike the Postgres helper, it understands the
// concrete Go types the ClickHouse driver returns: small/unsigned integers are widened
// to int64, uint64 values that overflow int64 are stored as strings (go-client cannot
// represent them), and arrays/maps of any element type are converted recursively.
func clickhouseSanitizeValue(val any) any {
	switch v := val.(type) {
	case nil, bool, string, []byte, float32, float64, int64:
		return v
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint:
		if uint64(v) > math.MaxInt64 {
			return strconv.FormatUint(uint64(v), 10)
		}
		return int64(v)
	case uint64:
		if v > math.MaxInt64 {
			return strconv.FormatUint(v, 10)
		}
		return int64(v)
	case time.Time:
		return v.Format(time.RFC3339)
	}

	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return clickhouseSanitizeValue(rv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		n := rv.Len()
		out := make([]any, n)
		for i := 0; i < n; i++ {
			out[i] = clickhouseSanitizeValue(rv.Index(i).Interface())
		}
		return out
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		for _, k := range rv.MapKeys() {
			out[fmt.Sprint(k.Interface())] = clickhouseSanitizeValue(rv.MapIndex(k).Interface())
		}
		return out
	default:
		// Fallback for types like big.Int, decimal.Decimal, net.IP, uuid.UUID, etc.
		return fmt.Sprint(val)
	}
}

// toFloat32Slice converts a ClickHouse array value into a []float32 vector.
// A nil value yields an empty slice so the caller can treat it as an empty vector.
func toFloat32Slice(val any) ([]float32, error) {
	switch v := val.(type) {
	case nil:
		return nil, nil
	case []float32:
		return v, nil
	case []float64:
		out := make([]float32, len(v))
		for i, f := range v {
			out[i] = float32(f)
		}
		return out, nil
	case []any:
		out := make([]float32, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float32:
				out[i] = n
			case float64:
				out[i] = float32(n)
			default:
				return nil, fmt.Errorf("unsupported vector element type %T", item)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected an array type, got %T", val)
	}
}
