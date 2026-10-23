package cmd

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getPort(t *testing.T) {
	tests := []struct {
		name     string
		url      *url.URL
		expected int
	}{
		{
			name:     "tls enabled, custom port",
			url:      &url.URL{Scheme: "https", Host: "localhost:6334"},
			expected: 6334,
		},
		{
			name:     "tls enabled, default port",
			url:      &url.URL{Scheme: "https", Host: "localhost"},
			expected: 443,
		},
		{
			name:     "tls disabled, default port",
			url:      &url.URL{Scheme: "http", Host: "localhost"},
			expected: 80,
		},
		{
			name:     "tls disabled, custom port",
			url:      &url.URL{Scheme: "http", Host: "localhost:6334"},
			expected: 6334,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := getPort(tt.url)
			if got != tt.expected {
				t.Errorf("getPort() got = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestQdrantServerlessFlags(t *testing.T) {
	ctx, err := NewParser([]string{
		"qdrant",
		"--source.url", "https://source.example.cloud.qdrant.io",
		"--source.collection", "source",
		"--source.serverless",
		"--target.url", "https://target.example.cloud.qdrant.io",
		"--target.collection", "target",
		"--target.serverless",
	})
	require.NoError(t, err)

	cmd, ok := ctx.Selected().Target.Addr().Interface().(*MigrateFromQdrantCmd)
	require.True(t, ok)
	require.True(t, cmd.Source.Serverless)
	require.True(t, cmd.Target.Serverless)

	require.NoError(t, cmd.Parse())
	require.Equal(t, DefaultHTTPSPort, cmd.sourcePort)
	require.True(t, cmd.sourceTLS)
	require.Equal(t, DefaultHTTPSPort, cmd.targetPort)
	require.True(t, cmd.targetTLS)
}

func TestQdrantServerlessValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		err  string
	}{
		{
			name: "shard key with a serverless target",
			args: []string{"--target.serverless", "--target.shard-key", "tier-a"},
			err:  "cannot be used with a Qdrant Serverless target",
		},
		{
			// Qdrant Serverless returns at most 1000 points for one request.
			name: "batch size above 1000 with a serverless source",
			args: []string{"--source.serverless", "--migration.batch-size", "1001"},
			err:  "batch size must be at most 1000 with a Qdrant Serverless source",
		},
		{
			name: "batch size of 1000 with a serverless source",
			args: []string{"--source.serverless", "--migration.batch-size", "1000"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewParser(append([]string{
				"qdrant",
				"--source.url", "https://source.example.cloud.qdrant.io",
				"--source.collection", "source",
				"--target.url", "https://target.example.cloud.qdrant.io",
				"--target.collection", "target",
			}, tt.args...))
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.err)
		})
	}
}
