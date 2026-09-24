package cmd

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/qdrant/migration/pkg/commons"
)

//go:embed lancedb_to_qdrant.py
var lanceDBScript string

type LanceDBConfig struct {
	URI           string   `name:"uri" required:"" help:"LanceDB directory, object-store URI, or db:// Cloud URI."`
	Table         string   `required:"" help:"Source table name."`
	IDColumn      string   `name:"id-column" required:"" help:"Unique, non-null string or integer column."`
	VectorColumns []string `help:"Comma-separated dense vector columns; each becomes a named Qdrant vector." default:"vector"`
	Version       int64    `help:"Source version; 0 uses the saved checkpoint version or latest on a new run." default:"0"`
	APIKey        string   `name:"api-key" env:"LANCEDB_API_KEY" help:"LanceDB Cloud API key."`
	Region        string   `help:"LanceDB Cloud region." default:"us-east-1"`
	StagingDir    string   `help:"Directory for temporary SQLite staging; defaults to the system temp directory."`
}

type MigrateFromLanceDBCmd struct {
	LanceDB        LanceDBConfig           `embed:"" prefix:"lancedb."`
	Qdrant         commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration      commons.MigrationConfig `embed:"" prefix:"migration."`
	DistanceMetric map[string]string       `prefix:"qdrant." help:"Vector column=distance mappings (cosine,dot,euclid,manhattan); default cosine."`
}

func (r *MigrateFromLanceDBCmd) Validate() error {
	if err := validateBatchSize(r.Migration.BatchSize); err != nil {
		return err
	}
	if r.Migration.BatchDelay < 0 || r.LanceDB.Version < 0 {
		return fmt.Errorf("batch delay and LanceDB version must be non-negative")
	}
	if r.Qdrant.Collection == r.Migration.OffsetsCollection {
		return fmt.Errorf("target and offsets collections must be different")
	}
	return nil
}

func (r *MigrateFromLanceDBCmd) helperConfig() map[string]any {
	return map[string]any{
		"uri": r.LanceDB.URI, "table": r.LanceDB.Table,
		"id_column": r.LanceDB.IDColumn, "vector_columns": r.LanceDB.VectorColumns,
		"version": r.LanceDB.Version, "source_api_key": r.LanceDB.APIKey,
		"region": r.LanceDB.Region, "staging_dir": r.LanceDB.StagingDir,
		"qdrant_url": r.Qdrant.Url, "qdrant_api_key": r.Qdrant.APIKey,
		"collection": r.Qdrant.Collection, "metrics": r.DistanceMetric,
		"batch_size": r.Migration.BatchSize, "batch_delay": r.Migration.BatchDelay,
		"restart": r.Migration.Restart, "create_collection": r.Migration.CreateCollection,
		"offsets_collection": r.Migration.OffsetsCollection,
	}
}

func (r *MigrateFromLanceDBCmd) Run(globals *Globals) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if globals.SkipTlsVerification {
		return fmt.Errorf("lancedb does not support --skip-tls-verification; use a trusted CA via GRPC_DEFAULT_SSL_ROOTS_FILE_PATH")
	}
	config, err := json.Marshal(r.helperConfig())
	if err != nil {
		return fmt.Errorf("encode LanceDB migration configuration: %w", err)
	}
	python, err := getPythonPath()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmd := exec.CommandContext(ctx, python, "-u", "-c", lanceDBScript)
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	cmd.Stdin = bytes.NewReader(config)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("LanceDB migration failed (see helper error above): %w", err)
	}
	return nil
}
