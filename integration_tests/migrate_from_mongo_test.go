package integrationtests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/qdrant/go-client/qdrant"
)

func TestMigrateFromMongo(t *testing.T) {
	ctx := context.Background()

	qdrantCont := qdrantContainer(ctx, t, qdrantAPIKey)
	mongoCont := mongoContainer(ctx, t)

	t.Cleanup(func() {
		require.NoError(t, qdrantCont.Terminate(ctx))
		require.NoError(t, mongoCont.Terminate(ctx))
	})

	mongoHost, err := mongoCont.PortEndpoint(ctx, "27017/tcp", "")
	require.NoError(t, err)

	qdrantHost, err := qdrantCont.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := qdrantCont.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)
	qdrantPort := mappedPort.Int()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", mongoHost)))
	require.NoError(t, err)
	defer func() {
		err := mongoClient.Disconnect(ctx)
		require.NoError(t, err)
	}()

	db := mongoClient.Database("testdb")
	coll := db.Collection("testcoll")

	testIDs := make([]string, totalEntries)
	testVectors := make([][]float32, totalEntries)
	testDocs := make([]string, totalEntries)
	testSources := make([]string, totalEntries)

	for i := 0; i < totalEntries; i++ {
		testIDs[i] = fmt.Sprintf("%d", i+1)
		testVectors[i] = randFloat32Values(dimension)
		testDocs[i] = fmt.Sprintf("test doc %d", i+1)
		testSources[i] = fmt.Sprintf("source%d", i+1)
		_, err := coll.InsertOne(ctx, bson.M{
			// _id is a mandatory field in MongoDB, so we use it to store the ID.
			// If not specified, MongoDB will generate a random ObjectID.
			"_id":    testIDs[i],
			"vector": testVectors[i],
			"doc":    testDocs[i],
			"source": testSources[i],
		})
		require.NoError(t, err)
	}

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:   qdrantHost,
		Port:   qdrantPort,
		APIKey: qdrantAPIKey,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	err = qdrantClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: testCollectionName,
		VectorsConfig: qdrant.NewVectorsConfigMap(
			map[string]*qdrant.VectorParams{
				"vector": {
					Size:     uint64(dimension),
					Distance: qdrant.Distance_Dot,
				},
			},
		),
	})
	require.NoError(t, err)

	args := []string{
		"mongodb",
		fmt.Sprintf("--mongodb.url=mongodb://%s", mongoHost),
		"--mongodb.database=testdb",
		"--mongodb.collection=testcoll",
		fmt.Sprintf("--qdrant.url=http://%s:%d", qdrantHost, qdrantPort),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
	}

	runMigrationBinary(t, args)

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(len(testIDs))),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, len(testIDs))

	expectedPoints := make(map[string]struct {
		doc    string
		source string
		vector []float32
	})
	for i, id := range testIDs {
		expectedPoints[id] = struct {
			doc    string
			source string
			vector []float32
		}{
			doc:    testDocs[i],
			source: testSources[i],
			vector: testVectors[i],
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		expected, exists := expectedPoints[id]
		require.True(t, exists)
		require.Equal(t, expected.doc, point.Payload["doc"].GetStringValue())
		require.Equal(t, expected.source, point.Payload["source"].GetStringValue())
		vector := point.Vectors.GetVectors().GetVectors()["vector"].GetData()
		require.Equal(t, expected.vector, vector)
	}
}
