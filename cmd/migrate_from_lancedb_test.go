package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/qdrant/migration/pkg/commons"
)

func TestLanceDBCLI(t *testing.T) {
	ctx, err := NewParser([]string{
		"lancedb", "--lancedb.uri=/tmp/source", "--lancedb.table=documents",
		"--lancedb.id-column=id", "--lancedb.vector-columns=text,image",
		"--lancedb.version=7", "--qdrant.collection=target",
		"--qdrant.distance-metric=text=cosine,image=dot",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Command() != "lancedb" {
		t.Fatalf("unexpected command: %s", ctx.Command())
	}
}

func TestLanceDBCLIRequiresSourceFields(t *testing.T) {
	_, err := NewParser([]string{"lancedb", "--qdrant.collection=target"})
	if err == nil {
		t.Fatal("expected required source flags to be enforced")
	}
}

func TestLanceDBValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*MigrateFromLanceDBCmd)
	}{
		{"zero batch", func(r *MigrateFromLanceDBCmd) { r.Migration.BatchSize = 0 }},
		{"negative delay", func(r *MigrateFromLanceDBCmd) { r.Migration.BatchDelay = -1 }},
		{"negative version", func(r *MigrateFromLanceDBCmd) { r.LanceDB.Version = -1 }},
		{"offset target collision", func(r *MigrateFromLanceDBCmd) { r.Qdrant.Collection = "offsets" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := MigrateFromLanceDBCmd{
				Qdrant:    commons.QdrantConfig{Collection: "target"},
				Migration: commons.MigrationConfig{BatchSize: 10, OffsetsCollection: "offsets"},
			}
			tc.edit(&r)
			if r.Validate() == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLanceDBHelperConfig(t *testing.T) {
	r := MigrateFromLanceDBCmd{
		LanceDB:        LanceDBConfig{URI: "/tmp/source with spaces", Table: "docs", IDColumn: "id", VectorColumns: []string{"vector"}, Version: 9, APIKey: "source-secret"},
		Qdrant:         commons.QdrantConfig{Collection: "target", Url: "https://example.com:6334", APIKey: "target-secret"},
		Migration:      commons.MigrationConfig{BatchSize: 23, Restart: true, CreateCollection: false, OffsetsCollection: "offsets", BatchDelay: 10},
		DistanceMetric: map[string]string{"vector": "dot"},
	}
	encoded, err := json.Marshal(r.helperConfig())
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(encoded, &config); err != nil {
		t.Fatal(err)
	}
	if config["uri"] != r.LanceDB.URI || config["version"] != float64(9) || config["batch_size"] != float64(23) || config["restart"] != true || config["create_collection"] != false || config["qdrant_api_key"] != "target-secret" {
		t.Fatalf("helper configuration lost CLI values: keys=%d", len(config))
	}
	if !strings.Contains(lanceDBScript, "def migrate(") {
		t.Fatal("Python migration helper was not embedded")
	}
}
