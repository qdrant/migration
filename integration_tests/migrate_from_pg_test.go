package integrationtests

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const (
	pgTable        = "embeddings_table"
	pgVectorColumn = "embedding"
	pgKeyColumn    = "id"
)

type pgEntry struct {
	name      string
	age       int64
	height    float64
	isActive  bool
	embedding []float32
}

func testMigrateFromPG(t *testing.T, collectionName string, numWorkers int) {
	ctx := context.Background()

	pgConnStr, qdrantUrl, qdrantHost, qdrantPort := setupPGQdrantContainers(t, ctx)

	expected := setupPGTable(ctx, t, pgConnStr)

	args := []string{
		"pg",
		fmt.Sprintf("--pg.url=%s", pgConnStr),
		fmt.Sprintf("--pg.table=%s", pgTable),
		fmt.Sprintf("--pg.key-column=%s", pgKeyColumn),
		fmt.Sprintf("--qdrant.url=%s", qdrantUrl),
		fmt.Sprintf("--qdrant.collection=%s", collectionName),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.distance-metric=%s=dot", pgVectorColumn),
		fmt.Sprintf("--migration.num-workers=%d", numWorkers),
	}

	runMigrationBinary(t, args)

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   qdrantPort,
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer client.Close()

	points, err := client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: collectionName,
		Limit:          qdrant.PtrOf(uint32(totalEntries)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, totalEntries)

	for _, point := range points {
		payload := point.Payload
		id := payload[pgKeyColumn].GetIntegerValue()
		exp := expected[id]
		require.Equal(t, exp.name, payload["name"].GetStringValue())
		require.Equal(t, exp.age, payload["age"].GetIntegerValue())
		require.InEpsilon(t, exp.height, payload["height"].GetDoubleValue(), 1e-2)
		require.Equal(t, exp.isActive, payload["is_active"].GetBoolValue())
		vec := point.Vectors.GetVectors().GetVectors()[pgVectorColumn].GetDenseVector().Data
		require.Equal(t, exp.embedding, vec)
	}
}

func TestMigrateFromPG(t *testing.T) {
	testMigrateFromPG(t, testCollectionName, 1)
}

func TestMigrateFromPGParallel(t *testing.T) {
	testMigrateFromPG(t, testCollectionName, 4)
}

func setupPGTable(ctx context.Context, t *testing.T, connStr string) []pgEntry {
	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s INTEGER PRIMARY KEY,
		name TEXT,
		age INTEGER,
		height DOUBLE PRECISION,
		is_active BOOLEAN,
		embedding VECTOR(%d)
	)`, pgTable, pgKeyColumn, dimension))
	require.NoError(t, err)

	expected := make([]pgEntry, totalEntries)

	for i := range totalEntries {
		name := fmt.Sprintf("Entry %d", i)
		age := int64(rand.Intn(100))
		height := rand.Float64()
		isActive := rand.Intn(2) == 0
		vec := randFloat32Values(dimension)
		embedding := pgvector.NewVector(vec)
		_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (%s, name, age, height, is_active, embedding) VALUES ($1, $2, $3, $4, $5, $6)`, pgTable, pgKeyColumn), i, name, age, height, isActive, embedding)
		require.NoError(t, err)
		expected[i] = pgEntry{
			name:      name,
			age:       age,
			height:    height,
			isActive:  isActive,
			embedding: vec,
		}
	}
	return expected
}

func setupPGQdrantContainers(t *testing.T, ctx context.Context) (pgConnStr, qdrantUrl, qdrantHost string, qdrantPort int) {
	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	pgCont := pgContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, pgCont.Terminate(ctx))
	})

	pgHost, err := pgCont.Host(ctx)
	require.NoError(t, err)
	pgPortObj, err := pgCont.MappedPort(ctx, "5432")
	require.NoError(t, err)
	pgConnStr = fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres", pgHost, pgPortObj.Port())

	qdrantHost, err = qdrantCont.Host(ctx)
	require.NoError(t, err)
	qdrantPortObj, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort = qdrantPortObj.Int()
	qdrantUrl = fmt.Sprintf("http://%s:%d", qdrantHost, qdrantPort)

	return
}
