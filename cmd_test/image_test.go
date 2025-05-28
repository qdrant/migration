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

func chromaContainer(ctx context.Context, t *testing.T) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "chromadb/chroma",
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

func pineconeContainer(ctx context.Context, t *testing.T) testcontainers.Container {

	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/pinecone-io/pinecone-local:latest",
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
