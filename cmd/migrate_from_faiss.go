package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

const PythonScript = "cmd/faiss_to_qdrant.py"

type MigrateFromFaissCmd struct {
	FaissIndex     commons.FaissConfig     `embed:"" prefix:"faiss."`
	Qdrant         commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration      commons.MigrationConfig `embed:"" prefix:"migration."`
	DistanceMetric string                  `prefix:"qdrant." enum:"euclid,cosine,dot,manhattan" help:"Distance metric for the Qdrant collection" default:"cosine"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromFaissCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromFaissCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromFaissCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("FAISS to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	dim, err := r.getFaissDimension(ctx)
	if err != nil {
		return err
	}

	if err = r.prepareTargetCollection(ctx, dim, targetClient); err != nil {
		return err
	}

	total, err := r.getFaissTotal(ctx)
	if err != nil {
		return err
	}

	displayMigrationStart("faiss", r.FaissIndex.IndexPath, r.Qdrant.Collection)
	if err = r.migrateData(ctx, targetClient, total); err != nil {
		pterm.Error.Printfln("Migration failed: %v", err)
		return err
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Qdrant.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	pterm.Info.Printfln("Target collection has %d points\n", targetPointCount)
	return nil
}

func (r *MigrateFromFaissCmd) getFaissDimension(ctx context.Context) (int, error) {
	pythonPath, err := exec.LookPath("python3")
	if err != nil {
		return 0, fmt.Errorf("python not found in PATH")
	}

	dimCmd := exec.CommandContext(ctx, pythonPath, PythonScript, "--action", "get-dim", "--faiss-index", r.FaissIndex.IndexPath)
	dimOut, err := dimCmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get dimension from FAISS index: %w", err)
	}
	var dim int
	_, err = fmt.Sscanf(string(dimOut), "%d", &dim)
	if err != nil || dim <= 0 {
		return 0, fmt.Errorf("invalid dimension returned from FAISS index: %s", string(dimOut))
	}
	return dim, nil
}

func (r *MigrateFromFaissCmd) getFaissTotal(ctx context.Context) (int, error) {
	pythonPath, err := exec.LookPath("python3")
	if err != nil {
		return 0, fmt.Errorf("python3 not found in PATH")
	}

	totalCmd := exec.CommandContext(ctx, pythonPath, PythonScript, "--action", "get-total", "--faiss-index", r.FaissIndex.IndexPath)
	totalOut, err := totalCmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get total vector count from FAISS index: %w", err)
	}
	var total int
	_, err = fmt.Sscanf(string(totalOut), "%d", &total)
	if err != nil || total <= 0 {
		return 0, fmt.Errorf("invalid total returned from FAISS index: %s", string(totalOut))
	}
	return total, nil
}

func (r *MigrateFromFaissCmd) prepareTargetCollection(ctx context.Context, dim int, targetClient *qdrant.Client) error {
	if !r.Migration.CreateCollection {
		return nil
	}
	exists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if exists {
		pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", r.Qdrant.Collection)
		return nil
	}
	distanceMapping := map[string]qdrant.Distance{
		"euclid":    qdrant.Distance_Euclid,
		"cosine":    qdrant.Distance_Cosine,
		"dot":       qdrant.Distance_Dot,
		"manhattan": qdrant.Distance_Manhattan,
	}
	createReq := &qdrant.CreateCollection{
		CollectionName: r.Qdrant.Collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(dim),
			Distance: distanceMapping[r.DistanceMetric],
		}),
	}
	if err := targetClient.CreateCollection(ctx, createReq); err != nil {
		return fmt.Errorf("failed to create target collection: %w", err)
	}
	pterm.Success.Printfln("Created target collection '%s'", r.Qdrant.Collection)
	return nil
}

func (r *MigrateFromFaissCmd) migrateData(ctx context.Context, targetClient *qdrant.Client, total int) error {
	var currentOffset uint64 = 0
	if !r.Migration.Restart {
		_, offsetStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.FaissIndex.IndexPath)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		currentOffset = offsetStored
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(total).Start()
	displayMigrationProgress(bar, currentOffset)

	pythonPath, err := exec.LookPath("python3")
	if err != nil {
		return fmt.Errorf("python3 not found in PATH")
	}

	args := []string{
		PythonScript,
		"--action", "migrate",
		"--faiss-index", r.FaissIndex.IndexPath,
		"--qdrant-url", r.Qdrant.Url,
		"--collection", r.Qdrant.Collection,
		"--batch-size", fmt.Sprintf("%d", r.Migration.BatchSize),
		"--offset", fmt.Sprintf("%d", currentOffset),
	}
	if r.Qdrant.APIKey != "" {
		args = append(args, "--qdrant-api-key", r.Qdrant.APIKey)
	}

	cmd := exec.CommandContext(ctx, pythonPath, args...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("faiss migration failed to start: %w", err)
	}

	scanner := bufio.NewScanner(stdoutPipe)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "OFFSET:") {
			offsetStr := strings.TrimPrefix(line, "OFFSET:")
			var newOffset uint64
			_, err := fmt.Sscanf(offsetStr, "%d", &newOffset)
			if err == nil {
				// Just a placeholder ID for offset tracking.
				// We're only using the offset count
				offsetId := qdrant.NewIDNum(0)
				err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.FaissIndex.IndexPath, offsetId, newOffset)
				if err != nil {
					pterm.Warning.Printfln("Failed to store offset: %v", err)
				}
				bar.Add(int(newOffset - currentOffset))
				currentOffset = newOffset
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading migration output: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("faiss migration failed: %w", err)
	}
	return nil
}
