package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseConnectionString(t *testing.T) {
	testCases := []struct {
		connStr        string
		connectionType string
		host           string
		port           int
		collection     string
		tls            bool
		apiKey         string
		errorMsg       string
	}{
		{
			connStr:        "qdrant:///http://localhost:6333/collection",
			connectionType: "qdrant",
			host:           "localhost",
			port:           6333,
			collection:     "collection",
			tls:            false,
			apiKey:         "",
			errorMsg:       "",
		},
		{
			connStr:        "qdrant:///https://foo.bar-baz.com:6334/collection_name",
			connectionType: "qdrant",
			host:           "foo.bar-baz.com",
			port:           6334,
			collection:     "collection_name",
			tls:            true,
			apiKey:         "",
			errorMsg:       "",
		},
		{
			connStr:        "qdrant:///https://foo.bar-baz.com:6334/collection_name?apiKey=test",
			connectionType: "qdrant",
			host:           "foo.bar-baz.com",
			port:           6334,
			collection:     "collection_name",
			tls:            true,
			apiKey:         "test",
			errorMsg:       "",
		},
		{
			connStr:        "qdrant:///https://foo.bar-baz.com:6334",
			connectionType: "",
			host:           "",
			port:           0,
			collection:     "",
			tls:            false,
			apiKey:         "",
			errorMsg:       "failed to parse connection string: qdrant:///https://foo.bar-baz.com:6334",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.connStr, func(t *testing.T) {
			connectionType, host, port, collection, tls, apiKey, err := parseConnectionString(tc.connStr)
			if err != nil {
				assert.Equal(t, tc.errorMsg, err.Error())
			}
			assert.Equal(t, tc.connectionType, connectionType)
			assert.Equal(t, tc.host, host)
			assert.Equal(t, tc.port, port)
			assert.Equal(t, tc.collection, collection)
			assert.Equal(t, tc.tls, tls)
			assert.Equal(t, tc.apiKey, apiKey)
		})

	}

}
