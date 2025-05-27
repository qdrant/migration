package cmd_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func qdrantContainer(ctx context.Context, t *testing.T, apiKey string) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "qdrant/qdrant",
		ExposedPorts: []string{"6334/tcp"},
		Env: map[string]string{
			"QDRANT__SERVICE__API_KEY": apiKey,
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("6334/tcp").WithStartupTimeout(5 * time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	return container
}

func weaviateContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "cr.weaviate.io/semitechnologies/weaviate:1.30.4",
		ExposedPorts: []string{"8080/tcp"},
		Env: map[string]string{
			"QUERY_DEFAULTS_LIMIT":                    "25",
			"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
			"DEFAULT_VECTORIZER_MODULE":               "none",
			"CLUSTER_HOSTNAME":                        "node1",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("8080/tcp").WithStartupTimeout(5 * time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	return container
}
