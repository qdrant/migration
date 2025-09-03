package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/vectorsearch"
	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromDatabricksCmd struct {
	Databricks commons.DatabricksConfig `embed:"" prefix:"databricks."`
	Qdrant     commons.QdrantConfig     `embed:"" prefix:"qdrant."`
	Migration  commons.MigrationConfig  `embed:"" prefix:"migration."`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromDatabricksCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromDatabricksCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromDatabricksCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Databricks Vector Search to Qdrant Data Migration")

	if err := r.Parse(); err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w := databricks.Must(databricks.NewWorkspaceClient())

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Qdrant.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant target: %w", err)
	}
	defer targetClient.Close()

	targetCollectionExists, err := targetClient.CollectionExists(ctx, r.Qdrant.Collection)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if !targetCollectionExists {
		return fmt.Errorf("target collection '%s' does not exist in Qdrant", r.Qdrant.Collection)
	}

	if err := commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient); err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	displayMigrationStart("databricks", r.Databricks.IndexName, r.Qdrant.Collection)

	if err := r.migrateData(ctx, w, targetClient); err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{CollectionName: r.Qdrant.Collection, Exact: qdrant.PtrOf(true)})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}
	pterm.Info.Printfln("Target collection has %d points", targetPointCount)
	return nil
}

func (r *MigrateFromDatabricksCmd) migrateData(ctx context.Context, w *databricks.WorkspaceClient, targetClient *qdrant.Client) error {
	batchSize := r.Migration.BatchSize

	var offsetId *qdrant.PointId
	offsetCount := uint64(0)
	if !r.Migration.Restart {
		id, storedOffset, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Databricks.IndexName)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetId = id
		offsetCount = storedOffset
	}

	// We don't know total count upfront. Hence using a spinner instead of a progress bar.
	// Similar to S3 Vectors migration.
	spinner, _ := pterm.DefaultSpinner.Start("Migrating data")
	if offsetCount > 0 {
		spinner.UpdateText(fmt.Sprintf("Resuming migration from %d points", offsetCount))
	}

	lastPrimaryKey := ""
	if offsetId != nil {
		lastPrimaryKey = offsetId.GetUuid()
	}

	for {
		resp, err := w.VectorSearchIndexes.ScanIndex(ctx, vectorsearch.ScanVectorIndexRequest{
			IndexName:      r.Databricks.IndexName,
			NumResults:     int(batchSize),
			LastPrimaryKey: lastPrimaryKey,
		})
		if err != nil {
			return fmt.Errorf("scan index failed: %w", err)
		}

		if len(resp.Data) == 0 {
			break
		}

		var points []*qdrant.PointStruct
		for _, row := range resp.Data {
			namedVectors, payload := extractNamedVectorsAndPayload(row)
			qdrantVectors := make(map[string]*qdrant.Vector, len(namedVectors))
			for name, vec := range namedVectors {
				qdrantVectors[name] = qdrant.NewVector(vec...)
			}

			pkRaw, ok := payload[r.Databricks.PrimaryKey]
			if !ok {
				return fmt.Errorf("value for primary key %s not found in row", r.Databricks.PrimaryKey)
			}

			pt := &qdrant.PointStruct{
				Id:      arbitraryIDToUUID(fmt.Sprint(pkRaw)),
				Vectors: qdrant.NewVectorsMap(qdrantVectors),
			}
			qPayload := qdrant.NewValueMap(payload)
			pt.Payload = qPayload
			points = append(points, pt)
		}

		if len(points) > 0 {
			_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
				CollectionName: r.Qdrant.Collection,
				Points:         points,
				Wait:           qdrant.PtrOf(true),
			})
			if err != nil {
				return fmt.Errorf("failed to insert data into target: %w", err)
			}

			spinner.UpdateText(fmt.Sprintf("Migrated %d points", offsetCount+uint64(len(points))))
		}

		if resp.LastPrimaryKey != "" {
			offsetCount += uint64(len(points))
			offsetId = qdrant.NewID(resp.LastPrimaryKey)
			if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Databricks.IndexName, offsetId, offsetCount); err != nil {
				return fmt.Errorf("failed to store offset: %w", err)
			}
			lastPrimaryKey = resp.LastPrimaryKey
		} else {
			break
		}
	}

	spinner.Success("Migration finished successfully")
	return nil
}

// Extract all numeric list fields as named vectors; others become payload.
func extractNamedVectorsAndPayload(row vectorsearch.Struct) (map[string][]float32, map[string]any) {
	namedVectors := make(map[string][]float32)
	payload := make(map[string]any)

	for _, entry := range row.Fields {
		key := entry.Key
		val := entry.Value
		if val != nil && val.ListValue != nil && isNumericList(val.ListValue) {
			namedVectors[key] = listOfNumbersToFloat32(val.ListValue)
			continue
		}
		payload[key] = valueToAny(val)
	}
	return namedVectors, payload
}

func listOfNumbersToFloat32(lv *vectorsearch.ListValue) []float32 {
	out := make([]float32, len(lv.Values))
	for i, v := range lv.Values {
		out[i] = float32(v.NumberValue)
	}
	return out
}

func isNumericList(lv *vectorsearch.ListValue) bool {
	for i := range lv.Values {
		// We can't just check for NumberValue because it's 0 by default and 0 is valid vector value.
		if lv.Values[i].ListValue != nil || lv.Values[i].StructValue != nil || lv.Values[i].StringValue != "" || lv.Values[i].BoolValue {
			return false
		}
	}
	return true
}

func valueToAny(v *vectorsearch.Value) any {
	if v == nil {
		return nil
	}
	if v.ListValue != nil {
		arr := make([]any, len(v.ListValue.Values))
		for i := range v.ListValue.Values {
			arr[i] = valueToAny(&v.ListValue.Values[i])
		}
		return arr
	}
	if v.StructValue != nil {
		m := make(map[string]any, len(v.StructValue.Fields))
		for _, f := range v.StructValue.Fields {
			m[f.Key] = valueToAny(f.Value)
		}
		return m
	}
	if v.StringValue != "" {
		return v.StringValue
	}
	if v.NumberValue != 0 {
		return v.NumberValue
	}

	return v.BoolValue
}
