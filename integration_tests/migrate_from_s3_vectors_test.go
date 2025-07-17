package integrationtests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/document"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/stretchr/testify/require"

	"github.com/qdrant/go-client/qdrant"
)

func TestMigrateFromS3Vectors(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("Skipping S3 source test. AWS credentials not set")
	}

	ctx := context.Background()

	s3Bucket := "test-vector-bucket" + fmt.Sprintf("-%d", rand.Int())
	s3Index := "test-vector-index" + fmt.Sprintf("-%d", rand.Int())

	t.Log("Using S3 bucket:", s3Bucket)
	t.Log("Using S3 index:", s3Index)

	qdrantContainer := qdrantContainer(ctx, t, qdrantAPIKey)
	defer func() {
		if err := qdrantContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Qdrant container: %v", err)
		}
	}()

	qdrantHost, err := qdrantContainer.Host(ctx)
	require.NoError(t, err)
	qdrantPort, err := qdrantContainer.MappedPort(ctx, qdrantGRPCPort)
	require.NoError(t, err)

	sdkConfig, err := config.LoadDefaultConfig(ctx)
	require.NoError(t, err)
	s3Client := s3vectors.NewFromConfig(sdkConfig)

	_, err = s3Client.CreateVectorBucket(ctx, &s3vectors.CreateVectorBucketInput{
		VectorBucketName: qdrant.PtrOf(s3Bucket),
	})
	require.NoError(t, err)

	_, err = s3Client.CreateIndex(ctx, &s3vectors.CreateIndexInput{
		VectorBucketName: qdrant.PtrOf(s3Bucket),
		IndexName:        qdrant.PtrOf(s3Index),
		Dimension:        qdrant.PtrOf(int32(dimension)),
		DistanceMetric:   types.DistanceMetricEuclidean,
		DataType:         types.DataTypeFloat32,
	})
	require.NoError(t, err)

	vectors := make([]types.PutInputVector, totalEntries)
	for i := 0; i < totalEntries; i++ {
		key := fmt.Sprintf("vector-%d", i)
		vecValues := make([]float32, dimension)
		for j := 0; j < dimension; j++ {
			vecValues[j] = rand.Float32()
		}
		vectors[i] = types.PutInputVector{
			Key: qdrant.PtrOf(key),
			Metadata: document.NewLazyDocument(map[string]interface{}{
				"age":  i,
				"name": fmt.Sprintf("name-%d", i),
				"city": fmt.Sprintf("city-%d", i),
			}),
			Data: &types.VectorDataMemberFloat32{Value: vecValues},
		}
	}

	_, err = s3Client.PutVectors(ctx, &s3vectors.PutVectorsInput{
		VectorBucketName: qdrant.PtrOf(s3Bucket),
		IndexName:        qdrant.PtrOf(s3Index),
		Vectors:          vectors,
	})
	require.NoError(t, err)

	qdrantClient, err := qdrant.NewClient(&qdrant.Config{
		Host:   qdrantHost,
		Port:   qdrantPort.Int(),
		APIKey: qdrantAPIKey,
	})
	require.NoError(t, err)
	defer qdrantClient.Close()

	args := []string{
		"s3",
		fmt.Sprintf("--s3.bucket=%s", s3Bucket),
		fmt.Sprintf("--s3.index=%s", s3Index),
		fmt.Sprintf("--qdrant.url=http://%s:%s", qdrantHost, qdrantPort.Port()),
		fmt.Sprintf("--qdrant.collection=%s", testCollectionName),
		fmt.Sprintf("--qdrant.api-key=%s", qdrantAPIKey),
		fmt.Sprintf("--qdrant.id-field=%s", idField),
		fmt.Sprintf("--qdrant.dense-vector=%s", denseVectorField),
	}
	runMigrationBinary(t, args)

	points, err := qdrantClient.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: testCollectionName,
		Limit:          qdrant.PtrOf(uint32(totalEntries)),
		WithPayload:    qdrant.NewWithPayload(true),
		WithVectors:    qdrant.NewWithVectors(true),
	})
	require.NoError(t, err)
	require.Len(t, points, totalEntries)

	expected := make(map[string]struct {
		age    int64
		name   string
		city   string
		vector []float32
	})
	for i := 0; i < totalEntries; i++ {
		vec := vectors[i]
		vData := vec.Data.(*types.VectorDataMemberFloat32)
		expected[*vec.Key] = struct {
			age    int64
			name   string
			city   string
			vector []float32
		}{
			age:    int64(i),
			name:   fmt.Sprintf("name-%d", i),
			city:   fmt.Sprintf("city-%d", i),
			vector: vData.Value,
		}
	}

	for _, point := range points {
		id := point.Payload[idField].GetStringValue()
		exp, ok := expected[id]
		require.True(t, ok)
		payload := point.Payload
		require.Equal(t, exp.name, payload["name"].GetStringValue())
		require.Equal(t, exp.city, payload["city"].GetStringValue())
		require.Equal(t, exp.age, payload["age"].GetIntegerValue())
		vector := point.Vectors.GetVectors().GetVectors()[denseVectorField].GetData()
		require.Equal(t, exp.vector, vector)
	}

	_, err = s3Client.DeleteIndex(ctx, &s3vectors.DeleteIndexInput{
		VectorBucketName: qdrant.PtrOf(s3Bucket),
		IndexName:        qdrant.PtrOf(s3Index),
	})
	require.NoError(t, err)
	_, err = s3Client.DeleteVectorBucket(ctx, &s3vectors.DeleteVectorBucketInput{
		VectorBucketName: qdrant.PtrOf(s3Bucket),
	})
	require.NoError(t, err)
}
