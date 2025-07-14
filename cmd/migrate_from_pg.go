package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
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
	DistanceMetric map[string]string       `prefix:"qdrant." help:"Map of vector fields to distance metrics (cosine,dot,euclid,manhattan). Default cosine if missing."`

	targetHost string
	targetPort int
	targetTLS  bool
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
	row := conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", r.PG.Table))

	var count int64
	err := row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	return uint64(count), nil
}

func getVectorColumns(ctx context.Context, conn *pgx.Conn, table string) (map[string]uint64, error) {
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
	rows, err := conn.Query(ctx, query, table)
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
			selectColumns = strings.Join(r.PG.Columns, ", ")
		} else {
			selectColumns = "*"
		}
		query := fmt.Sprintf("SELECT %s FROM %s LIMIT $1 OFFSET $2", selectColumns, r.PG.Table)
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

		var targetPoints []*qdrant.PointStruct
		for _, row := range batchRows {
			point := &qdrant.PointStruct{}
			vectors := make(map[string]*qdrant.Vector)
			payload := make(map[string]interface{})

			for col, val := range row {
				if col == r.PG.KeyColumn {
					switch val.(type) {
					case int32, int64, string:
						// We can use values of above types to generate deterministic UUIDs
						// to be used as IDs in Qdrant.
					default:
						return fmt.Errorf("unsupported PK type for '%s': %T", col, val)
					}

					idStr := fmt.Sprint(val)
					point.Id = arbitraryIDToUUID(idStr)
					payload[col] = val
					continue
				}

				switch v := val.(type) {
				case pgvector.Vector:
					vectors[col] = qdrant.NewVector(v.Slice()...)
				case time.Time:
					payload[col] = v.Format(time.RFC3339)
				default:
					payload[col] = v
				}
			}

			if len(vectors) > 0 {
				point.Vectors = qdrant.NewVectorsMap(vectors)
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
