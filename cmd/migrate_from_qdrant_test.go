package cmd

import (
	"net/url"
	"testing"
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
