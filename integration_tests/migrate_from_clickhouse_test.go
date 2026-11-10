package integrationtests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

const (
	clickhouseTable        = "embeddings_table"
	clickhouseVectorColumn = "embedding"
	clickhouseKeyColumn    = "id"
)

type clickhouseEntry struct {
	name      string
	age       int64
	height    float64
	isActive  bool
	tags      []string
	embedding []float32
}

func TestMigrateFromClickHouse(t *testing.T) {
	ctx := context.Background()

	chAddr, qdrantUrl, qdrantHost, qdrantPort := setupClickHouseQdrantContainers(t, ctx)

	expected := setupClickHouseTable(ctx, t, chAddr)

	args := []string{
		"clickhouse",
		fmt.Sprintf("--clickhouse.addr=%s", chAddr),
		"--clickhouse.database=default",
		fmt.Sprintf("--clickhouse.table=%s", clickhouseTable),
		fmt.Sprintf("--clickhouse.key-column=%s", clickhouseKeyColumn),
		fmt.Sprintf("--clickhouse.vector-columns=%s", clickhouseVectorColumn),
		fmt.Sprintf("--qdrant.url=%s", qdrantUrl),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.distance-metric=%s=dot", clickhouseVectorColumn),
		"--migration.batch-size=32",
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
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(totalEntries)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, totalEntries)

	for _, point := range points {
		payload := point.Payload
		// Keys exceed int64, so they are stored as strings in the payload.
		id := payload[clickhouseKeyColumn].GetStringValue()
		require.NotEmpty(t, id)
		exp, ok := expected[id]
		require.True(t, ok, "unexpected id %q", id)

		require.Equal(t, exp.name, payload["name"].GetStringValue())
		require.Equal(t, exp.age, payload["age"].GetIntegerValue())
		require.InEpsilon(t, exp.height, payload["height"].GetDoubleValue(), 1e-2)
		require.Equal(t, exp.isActive, payload["is_active"].GetBoolValue())

		// Array(String) must be stored as a list of strings, not a stringified array.
		tagsList := payload["tags"].GetListValue().GetValues()
		require.Len(t, tagsList, len(exp.tags))
		for i, tv := range tagsList {
			require.Equal(t, exp.tags[i], tv.GetStringValue())
		}

		vec := point.Vectors.GetVectors().GetVectors()[clickhouseVectorColumn].GetDenseVector().GetData()
		require.Equal(t, exp.embedding, vec)
	}
}

func setupClickHouseTable(ctx context.Context, t *testing.T, addr string) map[string]clickhouseEntry {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: "default", Username: "default"},
	})
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s UInt64,
		name String,
		age Int64,
		height Float64,
		is_active Bool,
		tags Array(String),
		embedding Array(Float32)
	) ENGINE = MergeTree ORDER BY %s`, clickhouseTable, clickhouseKeyColumn, clickhouseKeyColumn)))

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", clickhouseTable))
	require.NoError(t, err)

	expected := make(map[string]clickhouseEntry, totalEntries)
	for i := range totalEntries {
		// Use values above 2^63 to exercise the large-UInt64 payload path.
		id := uint64(math.MaxUint64) - uint64(i)
		name := fmt.Sprintf("Entry %d", i)
		age := int64(rand.Intn(100))
		height := rand.Float64()
		isActive := rand.Intn(2) == 0
		tags := []string{fmt.Sprintf("tag-%d", i), "common"}
		vec := randFloat32Values(dimension)

		require.NoError(t, batch.Append(id, name, age, height, isActive, tags, vec))
		expected[fmt.Sprintf("%d", id)] = clickhouseEntry{
			name:      name,
			age:       age,
			height:    height,
			isActive:  isActive,
			tags:      tags,
			embedding: vec,
		}
	}
	require.NoError(t, batch.Send())

	return expected
}

func setupClickHouseQdrantContainers(t *testing.T, ctx context.Context) (chAddr, qdrantUrl, qdrantHost string, qdrantPort int) {
	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	chCont := clickhouseContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, chCont.Terminate(ctx))
	})

	chHost, err := chCont.Host(ctx)
	require.NoError(t, err)
	chPortObj, err := chCont.MappedPort(ctx, "9000")
	require.NoError(t, err)
	chAddr = fmt.Sprintf("%s:%s", chHost, chPortObj.Port())

	qdrantHost, err = qdrantCont.Host(ctx)
	require.NoError(t, err)
	qdrantPortObj, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort = int(qdrantPortObj.Num())
	qdrantUrl = fmt.Sprintf("http://%s:%d", qdrantHost, qdrantPort)

	return
}
