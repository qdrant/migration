//go:build !no_clickhouse

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"reflect"
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

func (r *MigrateFromClickHouseCmd) migrateData(ctx context.Context, conn driver.Conn, targetClient *qdrant.Client, sourcePointCount uint64) error {
	batchSize := r.Migration.BatchSize
	offsetCount := uint64(0)

	if !r.Migration.Restart {
		_, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.ClickHouse.Table)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetCount = count
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	keyColIdent := quoteClickHouseIdent(r.ClickHouse.KeyColumn)
	selectColumns := r.selectColumns()

	for {
		query := fmt.Sprintf(
			"SELECT %s FROM %s ORDER BY %s LIMIT ? OFFSET ?",
			selectColumns, r.tableIdentifier(), keyColIdent,
		)

		batchRows, err := r.queryBatch(ctx, conn, query, batchSize, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to query ClickHouse: %w", err)
		}

		if len(batchRows) == 0 {
			break
		}

		targetPoints, err := r.convertRowsToPoints(batchRows)
		if err != nil {
			return err
		}

		err = upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return err
		}

		// Just a placeholder ID. We're only using the offset count.
		offsetID := qdrant.NewIDNum(0)
		offsetCount += uint64(len(targetPoints))
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.ClickHouse.Table, offsetID, offsetCount)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))

		// Apply batch delay if configured (helps with rate limiting).
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
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

func (r *MigrateFromClickHouseCmd) convertRowsToPoints(batchRows []map[string]any) ([]*qdrant.PointStruct, error) {
	vectorSet := r.vectorColumnSet()

	targetPoints := make([]*qdrant.PointStruct, 0, len(batchRows))
	for _, row := range batchRows {
		point := &qdrant.PointStruct{}
		vectors := make(map[string]*qdrant.Vector)
		payload := make(map[string]any)

		for col, val := range row {
			if col == r.ClickHouse.KeyColumn {
				point.Id = arbitraryIDToUUID(keyToString(val))
			}

			if vectorSet[col] {
				vec, err := toFloat32Slice(val)
				if err != nil {
					return nil, fmt.Errorf("failed to convert column %q to a vector: %w", col, err)
				}
				vectors[col] = qdrant.NewVector(vec...)
			} else {
				payload[col] = sanitizeValue(val)
			}
		}

		if point.Id == nil {
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

// toFloat32Slice converts a ClickHouse array value into a []float32 vector.
func toFloat32Slice(val any) ([]float32, error) {
	switch v := val.(type) {
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
