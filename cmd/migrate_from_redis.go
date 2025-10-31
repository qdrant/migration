package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pterm/pterm"
	"github.com/redis/go-redis/v9"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromRedisCmd struct {
	Redis     commons.RedisConfig     `embed:"" prefix:"redis."`
	Qdrant    commons.QdrantConfig    `embed:"" prefix:"qdrant."`
	Migration commons.MigrationConfig `embed:"" prefix:"migration."`
	IdField   string                  `prefix:"qdrant." help:"Field storing Redis IDs in Qdrant." default:"__id__"`

	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromRedisCmd) Parse() error {
	var err error
	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Qdrant.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromRedisCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromRedisCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Redis Vector to Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr:       r.Redis.Addr,
		Username:   r.Redis.Username,
		Password:   r.Redis.Password,
		DB:         r.Redis.DB,
		Protocol:   r.Redis.Protocol,
		Network:    r.Redis.Network,
		ClientName: r.Redis.ClientName,
	})
	defer rdb.Close()

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

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	displayMigrationStart("redis", r.Redis.Index, r.Qdrant.Collection)

	sourcePointCount, err := r.countRedisDocuments(ctx, rdb)
	if err != nil {
		return fmt.Errorf("failed to count documents in Redis index: %w", err)
	}

	err = r.migrateData(ctx, rdb, targetClient, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
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

func (r *MigrateFromRedisCmd) countRedisDocuments(ctx context.Context, rdb *redis.Client) (uint64, error) {
	info, err := rdb.FTInfo(ctx, r.Redis.Index).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get Redis index info: %w", err)
	}

	pterm.Info.Printfln("Found Redis index '%s' with %d documents", r.Redis.Index, info.NumDocs)
	return uint64(info.NumDocs), nil
}

func (r *MigrateFromRedisCmd) migrateData(ctx context.Context, rdb *redis.Client, targetClient *qdrant.Client, sourcePointCount uint64) error {
	batchSize := r.Migration.BatchSize

	var currentOffset uint64 = 0

	if !r.Migration.Restart {
		_, offsetStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Redis.Index)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		currentOffset = offsetStored
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, currentOffset)

	info, err := rdb.FTInfo(ctx, r.Redis.Index).Result()
	if err != nil {
		return fmt.Errorf("failed to get index info: %w", err)
	}

	attrTypes := make(map[string]string)
	for _, attr := range info.Attributes {
		attrTypes[attr.Identifier] = attr.Type
	}

	for {
		res, err := rdb.FTSearchWithArgs(ctx, r.Redis.Index, "*", &redis.FTSearchOptions{
			LimitOffset: int(currentOffset),
			Limit:       int(batchSize),
		}).Result()
		if err != nil {
			return fmt.Errorf("failed to search Redis: %w", err)
		}

		count := len(res.Docs)
		if count == 0 {
			break
		}

		targetPoints := make([]*qdrant.PointStruct, 0, count)

		for i := 0; i < count; i++ {
			doc := res.Docs[i]

			parsedFields := make(map[string]interface{})
			vectorMap := make(map[string]*qdrant.Vector)

			for fieldName, rawVal := range doc.Fields {
				attrType := attrTypes[fieldName]

				if attrType == redis.SearchFieldTypeVector.String() {
					vec := bytesToFloats([]byte(rawVal))
					vectorMap[fieldName] = qdrant.NewVectorDense(vec)
				} else {
					parsedFields[fieldName] = parseFieldValue(attrType, rawVal)
				}
			}

			point := &qdrant.PointStruct{
				Id:      arbitraryIDToUUID(doc.ID),
				Vectors: qdrant.NewVectorsMap(vectorMap),
			}

			payload := qdrant.NewValueMap(parsedFields)
			payload[r.IdField] = qdrant.NewValueString(doc.ID)
			point.Payload = payload

			targetPoints = append(targetPoints, point)
		}

		if len(targetPoints) > 0 {
			_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
				CollectionName: r.Qdrant.Collection,
				Points:         targetPoints,
				Wait:           qdrant.PtrOf(true),
			})
			if err != nil {
				return fmt.Errorf("failed to insert data into target: %w", err)
			}
		}

		currentOffset += uint64(count)
		// Just a placeholder ID for offset tracking.
		// We're only using the offset count
		offsetId := qdrant.NewIDNum(0)
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, r.Redis.Index, offsetId, currentOffset)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(count)
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

// Ref: https://redis.io/docs/latest/develop/clients/go/vecsearch/#add-a-helper-function
func bytesToFloats(b []byte) []float32 {
	if len(b)%4 != 0 {
		log.Printf("Warning: byte slice length %d is not a multiple of 4, truncating", len(b))
		b = b[:len(b)-(len(b)%4)]
	}

	fs := make([]float32, len(b)/4)
	for i := 0; i < len(fs); i++ {
		bits := binary.LittleEndian.Uint32(b[i*4 : (i+1)*4])
		fs[i] = math.Float32frombits(bits)
	}
	return fs
}

func parseFieldValue(attrType string, val string) interface{} {
	// redis.SearchFieldTypeVector is handled
	// before invoking this function.
	if attrType == redis.SearchFieldTypeNumeric.String() {
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			log.Printf("Warning: failed to parse numeric value '%s': %v", val, err)
			return val
		}
		return f
	}
	return val
}
