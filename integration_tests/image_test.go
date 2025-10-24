package integrationtests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/opensearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

//nolint:unparam
func qdrantContainer(ctx context.Context, t *testing.T, apiKey string) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "qdrant/qdrant:v1.14.1",
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

func chromaContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "chromadb/chroma:1.0.12",
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("8000/tcp").WithStartupTimeout(5 * time.Second),
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
		Image:        "cr.weaviate.io/semitechnologies/weaviate:1.31.0",
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

func pineconeContainer(ctx context.Context, t *testing.T) testcontainers.Container {

	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/pinecone-io/pinecone-local:v1.0.0.rc0",
		ExposedPorts: []string{"5081/tcp", "5082/tcp"},
		Env: map[string]string{
			"PORT": "5081",
		},
		WaitingFor: wait.ForAll(wait.ForListeningPort("5081/tcp").WithStartupTimeout(30*time.Second),
			wait.ForListeningPort("5082/tcp").WithStartupTimeout(30*time.Second)),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	return container
}

func redisContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "redis/redis-stack:7.2.0-v17",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	return container
}

func mongoContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "mongo:6.0",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForListeningPort("27017/tcp").WithStartupTimeout(30 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	return container
}

func opensearchContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	container, err := opensearch.Run(ctx, "opensearchproject/opensearch:3.1.0")

	require.NoError(t, err)
	return container
}

func elasticsearchContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "elasticsearch:9.1.5",
		ExposedPorts: []string{"9200/tcp"},
		Env: map[string]string{
			"discovery.type":                    "single-node",
			"xpack.security.enabled":            "false",
			"xpack.security.enrollment.enabled": "false",
			"ES_JAVA_OPTS":                      "-Xms512m -Xmx512m",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9200/tcp").WithStartupTimeout(60*time.Second),
			wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(60*time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	return container
}

func pgContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "pgvector/pgvector:pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(30 * time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	return container
}
