package integrationtests

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/turbopuffer/turbopuffer-go"
	"github.com/turbopuffer/turbopuffer-go/option"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

const (
	turbopufferVectorName       = "vector"
	turbopufferOffsetCollection = "_migration_offsets"
	turbopufferCheckpoint       = 59
)

type turbopufferTestCase struct {
	name       string
	idType     string
	ids        []any // In ascending order, as turbopuffer pages through them.
	vectorType string
	metric     turbopuffer.DistanceMetric
	tolerance  float64
	resume     bool
}

func TestMigrateFromTurbopuffer(t *testing.T) {
	ctx := context.Background()
	apiKey := os.Getenv("TURBOPUFFER_API_KEY")
	if apiKey == "" {
		t.Skip("Skipping turbopuffer source test. TURBOPUFFER_API_KEY not set")
	}
	region := os.Getenv("TURBOPUFFER_REGION")
	if region == "" {
		t.Skip("Skipping turbopuffer source test. TURBOPUFFER_REGION not set")
	}
	client := turbopuffer.NewClient(option.WithAPIKey(apiKey), option.WithRegion(region))

	qdrantContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		require.NoError(t, qdrantContainer.Terminate(ctx))
	}()
	qdrantHost, err := qdrantContainer.Host(ctx)
	require.NoError(t, err)
	qdrantPort, err := qdrantContainer.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:                   qdrantHost,
		Port:                   int(qdrantPort.Num()),
		APIKey:                 qdrantAPIKey,
		SkipCompatibilityCheck: true,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	// Spaced so that about half of the IDs exceed math.MaxInt64.
	uintIDs := make([]any, totalEntries)
	stringIDs := make([]any, totalEntries)
	for i := range totalEntries {
		uintIDs[i] = uint64(i) * (math.MaxUint64 / totalEntries)
		stringIDs[i] = fmt.Sprintf("doc-%03d", i)
	}

	cases := []turbopufferTestCase{
		{"uint-f32-cosine", "uint", uintIDs, fmt.Sprintf("[%d]f32", dimension), turbopuffer.DistanceMetricCosineDistance, 1e-6, true},
		{"string-f16-euclidean", "string", stringIDs, fmt.Sprintf("[%d]f16", dimension), turbopuffer.DistanceMetricEuclideanSquared, 1e-3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			namespaceName := fmt.Sprintf("migration-test-%s-%d", tc.name, rand.Int())
			collectionName := testCollectionName + "-" + namespaceName
			vectors := writeTurbopufferNamespace(ctx, t, client.Namespace(namespaceName), tc)

			from := 0
			if tc.resume {
				checkpoint, err := json.Marshal(tc.ids[turbopufferCheckpoint])
				require.NoError(t, err)
				require.NoError(t, commons.PrepareOffsetsCollection(ctx, turbopufferOffsetCollection, qdrantClient))
				require.NoError(t, commons.StoreStartOffset(ctx, turbopufferOffsetCollection, qdrantClient,
					"turbopuffer/"+namespaceName, qdrant.NewID(string(checkpoint)), turbopufferCheckpoint+1))
				from = turbopufferCheckpoint + 1
			}

			args := []string{
				"turbopuffer",
				fmt.Sprintf("--turbopuffer.namespace=%s", namespaceName),
				fmt.Sprintf("--turbopuffer.api-key=%s", apiKey),
				fmt.Sprintf("--turbopuffer.region=%s", region),
				fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
				fmt.Sprintf("--qdrant.collection=%s", collectionName),
				fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
				fmt.Sprintf("--qdrant.id-field=%s", idField),
				fmt.Sprintf("--migration.offsets-collection=%s", turbopufferOffsetCollection),
				"--migration.batch-size=7",
			}
			runMigrationBinary(t, args)

			points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
				CollectionName: collectionName,
				Limit:          qdrant.PtrOf(uint32(totalEntries + 1)),
				WithPayload:    qdrant.NewWithPayload(true),
				WithVectors:    qdrant.NewWithVectors(true),
			})
			require.NoError(t, err)
			require.Len(t, points, len(tc.ids)-from)

			byID := make(map[string]*qdrant.RetrievedPoint, len(points))
			for _, point := range points {
				key, err := commons.EncodePointID(point.GetId())
				require.NoError(t, err)
				byID[key] = point
			}
			for i := from; i < len(tc.ids); i++ {
				key, err := commons.EncodePointID(turbopufferPointID(tc.ids[i]))
				require.NoError(t, err)
				point, ok := byID[key]
				require.True(t, ok, "point for turbopuffer ID %v not found", tc.ids[i])

				require.Equal(t, turbopufferIDPayload(tc.ids[i]), point.Payload[idField].GetKind())
				require.Equal(t, fmt.Sprintf("name-%d", i), point.Payload["name"].GetStringValue())
				require.IsType(t, &qdrant.Value_DoubleValue{}, point.Payload["score"].GetKind())
				require.InDelta(t, float64(i), point.Payload["score"].GetDoubleValue(), 0)

				expected := vectors[i]
				if tc.metric == turbopuffer.DistanceMetricCosineDistance {
					expected = normalizedVector(expected)
				}
				vector := point.Vectors.GetVectors().GetVectors()[turbopufferVectorName].GetDenseVector().GetData()
				require.InDeltaSlice(t, expected, vector, tc.tolerance)
			}
		})
	}
}

func writeTurbopufferNamespace(ctx context.Context, t *testing.T, namespace turbopuffer.Namespace, tc turbopufferTestCase) [][]float32 {
	t.Helper()

	// Never write to or delete a namespace this test did not create.
	_, err := namespace.Metadata(ctx, turbopuffer.NamespaceMetadataParams{})
	var apiErr *turbopuffer.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusNotFound, apiErr.StatusCode, "namespace %q already exists", namespace.ID())

	vectors := make([][]float32, len(tc.ids))
	rows := make([]turbopuffer.RowParam, len(tc.ids))
	for i, id := range tc.ids {
		vectors[i] = randFloat32Values(dimension)
		rows[i] = turbopuffer.RowParam{
			"id":                  id,
			turbopufferVectorName: vectors[i],
			"name":                fmt.Sprintf("name-%d", i),
			"score":               float64(i),
		}
	}
	t.Cleanup(func() {
		_, err := namespace.DeleteAll(context.WithoutCancel(ctx), turbopuffer.NamespaceDeleteAllParams{})
		require.NoError(t, err)
	})
	_, err = namespace.Write(ctx, turbopuffer.NamespaceWriteParams{
		UpsertRows:     rows,
		DistanceMetric: tc.metric,
		Schema: map[string]turbopuffer.AttributeSchemaConfigParam{
			"id":                  {Type: tc.idType},
			turbopufferVectorName: {Type: tc.vectorType, Ann: turbopuffer.AttributeSchemaConfigAnnParam{DistanceMetric: tc.metric}},
			"score":               {Type: "float"},
		},
	})
	require.NoError(t, err)
	return vectors
}

func turbopufferPointID(id any) *qdrant.PointId {
	if num, ok := id.(uint64); ok {
		return qdrant.NewIDNum(num)
	}
	return qdrant.NewIDUUID(uuid.NewSHA1(uuid.NameSpaceURL, []byte(id.(string))).String())
}

// Integers beyond int64 are stored as strings.
func turbopufferIDPayload(id any) any {
	num, ok := id.(uint64)
	if !ok {
		return &qdrant.Value_StringValue{StringValue: id.(string)}
	}
	if num <= math.MaxInt64 {
		return &qdrant.Value_IntegerValue{IntegerValue: int64(num)}
	}
	return &qdrant.Value_StringValue{StringValue: strconv.FormatUint(num, 10)}
}
