package cmd

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"syscall"
	"time"

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
	if r.Redis.Source == "ft" && r.Redis.Index == "" {
		return fmt.Errorf("--redis.index is required when --redis.source is 'ft'")
	}
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

	sourceName := r.Redis.Index
	if r.Redis.Source == "vectorset" {
		sourceName = r.Redis.KeyPattern
	}

	displayMigrationStart("redis", sourceName, r.Qdrant.Collection)

	sourcePointCount, err := r.countRedisDocuments(ctx, rdb)
	if err != nil {
		return fmt.Errorf("failed to count Redis source: %w", err)
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

	err = commons.DeleteOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to delete migration marker collection: %w", err)
	}

	return nil
}

func (r *MigrateFromRedisCmd) countRedisDocuments(ctx context.Context, rdb *redis.Client) (uint64, error) {
	if r.Redis.Source == "vectorset" {
		count, err := r.countVectorSetElements(ctx, rdb)
		if err != nil {
			return 0, err
		}

		pterm.Info.Printfln("Found %d elements in vector sets matching '%s'", count, r.Redis.KeyPattern)
		return count, nil
	}

	info, err := rdb.FTInfo(ctx, r.Redis.Index).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get Redis index info: %w", err)
	}

	pterm.Info.Printfln("Found Redis index '%s' with %d documents", r.Redis.Index, info.NumDocs)
	return uint64(info.NumDocs), nil
}

func (r *MigrateFromRedisCmd) listVectorSetKeys(ctx context.Context, rdb *redis.Client) ([]string, error) {
	keySet := make(map[string]struct{})
	var cursor uint64

	for {
		page, nextCursor, err := rdb.ScanType(ctx, cursor, r.Redis.KeyPattern, 100, "vectorset").Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan Redis keys: %w", err)
		}

		for _, key := range page {
			keySet[key] = struct{}{}
		}
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	// SCAN may return duplicates and does not guarantee ordering. A unique,
	// stable key order is required for an offset to identify the same element
	// after a restart.
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (r *MigrateFromRedisCmd) countVectorSetElements(ctx context.Context, rdb *redis.Client) (uint64, error) {
	keys, err := r.listVectorSetKeys(ctx, rdb)
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, key := range keys {
		card, err := rdb.VCard(ctx, key).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to get cardinality of vector set '%s': %w", key, err)
		}
		total += uint64(card)
	}

	return total, nil
}

func (r *MigrateFromRedisCmd) migrateData(ctx context.Context, rdb *redis.Client, targetClient *qdrant.Client, sourcePointCount uint64) error {
	if r.Redis.Source == "vectorset" {
		return r.migrateFromVectorSets(ctx, rdb, targetClient, sourcePointCount)
	}
	return r.migrateFromFTIndex(ctx, rdb, targetClient, sourcePointCount)
}

func (r *MigrateFromRedisCmd) migrateFromFTIndex(ctx context.Context, rdb *redis.Client, targetClient *qdrant.Client, sourcePointCount uint64) error {
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
			err = upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
				CollectionName: r.Qdrant.Collection,
				Points:         targetPoints,
				Wait:           qdrant.PtrOf(true),
			})
			if err != nil {
				return err
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

		// Apply batch delay if configured (helps with rate limiting)
		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

func (r *MigrateFromRedisCmd) migrateFromVectorSets(ctx context.Context, rdb *redis.Client, targetClient *qdrant.Client, sourcePointCount uint64) error {
	batchSize := r.Migration.BatchSize
	var currentOffset uint64
	var resumeCursor vectorSetCursor
	offsetKey := fmt.Sprintf("%s|%s", r.Redis.Source, r.Redis.KeyPattern)

	if !r.Migration.Restart {
		storedCursor, offsetStored, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		if storedCursor != nil {
			if err := json.Unmarshal([]byte(storedCursor.GetUuid()), &resumeCursor); err != nil {
				return fmt.Errorf("failed to decode stored Vector Set cursor. Use --migration.restart to start the migration over: %w", err)
			}
			if resumeCursor.Key == "" {
				return fmt.Errorf("stored Vector Set cursor has no key. Use --migration.restart to start the migration over")
			}
		}
		currentOffset = offsetStored
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, currentOffset)

	keys, err := r.listVectorSetKeys(ctx, rdb)
	if err != nil {
		return err
	}

	if resumeCursor.Key != "" {
		startKey := sort.SearchStrings(keys, resumeCursor.Key)
		if startKey == len(keys) || keys[startKey] != resumeCursor.Key {
			return fmt.Errorf("stored Vector Set key '%s' no longer exists. Use --migration.restart to start the migration over", resumeCursor.Key)
		}
		keys = keys[startKey:]
	}

	if len(keys) == 0 {
		pterm.Warning.Printfln("No vector sets match the key pattern '%s'", r.Redis.KeyPattern)
		return nil
	}

	targetPoints := make([]*qdrant.PointStruct, 0, batchSize)
	var currentCursor vectorSetCursor

	var tenantRegex *regexp.Regexp
	if r.Redis.TenantRegex != "" {
		tenantRegex, err = regexp.Compile(r.Redis.TenantRegex)
		if err != nil {
			return fmt.Errorf("failed to compile the tenant regex '%s': %w", r.Redis.TenantRegex, err)
		}
	}

	flushBatch := func() error {
		if len(targetPoints) == 0 {
			return nil
		}

		err := upsertWithRetry(ctx, targetClient, &qdrant.UpsertPoints{
			CollectionName: r.Qdrant.Collection,
			Points:         targetPoints,
			Wait:           qdrant.PtrOf(true),
		})
		if err != nil {
			return err
		}

		cursor, err := json.Marshal(currentCursor)
		if err != nil {
			return fmt.Errorf("failed to encode Vector Set cursor: %w", err)
		}
		err = commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, offsetKey, qdrant.NewIDUUID(string(cursor)), currentOffset)
		if err != nil {
			return fmt.Errorf("failed to store offset: %w", err)
		}

		bar.Add(len(targetPoints))
		targetPoints = make([]*qdrant.PointStruct, 0, batchSize)

		if r.Migration.BatchDelay > 0 {
			time.Sleep(time.Duration(r.Migration.BatchDelay) * time.Millisecond)
		}

		return nil
	}

	for _, key := range keys {
		dim, err := rdb.VDim(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("failed to get dimension of vector set '%s': %w", key, err)
		}

		tenantID := extractTenantID(tenantRegex, key)

		rangeStart := "-"
		if key == resumeCursor.Key {
			rangeStart = "(" + resumeCursor.Member
		}

		for {
			batchCapacity := batchSize - len(targetPoints)
			members, err := rdb.VRange(ctx, key, rangeStart, "+", int64(batchCapacity)).Result()
			if err != nil {
				return fmt.Errorf("failed to iterate members of vector set '%s' with VRANGE (Redis 8.4 or newer is required): %w", key, err)
			}
			if len(members) == 0 {
				break
			}

			points, err := r.readVectorSetMembers(ctx, rdb, key, members, int(dim), tenantID)
			if err != nil {
				return err
			}

			currentOffset += uint64(len(members))
			currentCursor = vectorSetCursor{Key: key, Member: members[len(members)-1]}
			rangeStart = "(" + currentCursor.Member
			targetPoints = append(targetPoints, points...)

			if len(targetPoints) >= batchSize {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}
	}

	if err := flushBatch(); err != nil {
		return err
	}

	pterm.Success.Printfln("Data migration finished successfully")
	return nil
}

type vectorSetCursor struct {
	Key    string `json:"key"`
	Member string `json:"member"`
}

func extractTenantID(tenantRegex *regexp.Regexp, key string) string {
	if tenantRegex == nil {
		return ""
	}

	match := tenantRegex.FindStringSubmatch(key)
	if len(match) > 1 {
		return match[1]
	}

	return ""
}

func (r *MigrateFromRedisCmd) readVectorSetMembers(ctx context.Context, rdb *redis.Client, key string, members []string, dim int, tenantID string) ([]*qdrant.PointStruct, error) {
	pipe := rdb.Pipeline()
	vectors := make([]*redis.SliceCmd, len(members))
	attrs := make([]*redis.StringCmd, len(members))
	for i, member := range members {
		// VEMB without RAW makes Redis de-normalize and de-quantize the stored
		// vector for us, so the tool does not decode the fp32/int8/bin blob
		// layouts itself. Ref: https://redis.io/docs/latest/commands/vemb/
		vectors[i] = pipe.VEmb(ctx, key, member, false)
		attrs[i] = pipe.VGetAttr(ctx, key, member)
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to retrieve elements from vector set '%s': %w", key, err)
	}

	points := make([]*qdrant.PointStruct, 0, len(members))
	for i, member := range members {
		rawVec, err := vectors[i].Result()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve embedding for '%s' in vector set '%s': %w", member, key, err)
		}

		attrStr, err := attrs[i].Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to retrieve attributes for '%s' in vector set '%s': %w", member, key, err)
		}
		if err == redis.Nil {
			attrStr = ""
		}

		vector, err := vectorReplyToFloats(rawVec, dim, key, member)
		if err != nil {
			return nil, err
		}

		payload := make(map[string]any)
		if attrStr != "" {
			if err := json.Unmarshal([]byte(attrStr), &payload); err != nil {
				return nil, fmt.Errorf("failed to parse attributes for '%s' in vector set '%s': %w", member, key, err)
			}
		}
		if tenantID != "" {
			payload[r.Redis.TenantField] = tenantID
		}

		sourceID := member
		if r.Redis.IDAttr != "" {
			if attrVal, ok := payload[r.Redis.IDAttr]; ok && attrVal != nil {
				sourceID = fmt.Sprintf("%v", attrVal)
			} else {
				log.Printf("Warning: ID attribute '%s' is missing for '%s' in vector set '%s'. The tool uses the element name as the point ID.", r.Redis.IDAttr, member, key)
			}
		}
		payload[r.IdField] = sourceID

		points = append(points, &qdrant.PointStruct{
			Id:      arbitraryIDToUUID(key + ":" + sourceID),
			Vectors: qdrant.NewVectors(vector...),
			Payload: qdrant.NewValueMap(payload),
		})
	}

	return points, nil
}

// vectorReplyToFloats converts a VEMB reply into a dense vector. Redis returns
// one component per array element, already de-normalized and de-quantized.
func vectorReplyToFloats(reply []interface{}, dim int, key, member string) ([]float32, error) {
	if len(reply) != dim {
		return nil, fmt.Errorf("VEMB for '%s' in vector set '%s' returned %d components, expected %d", member, key, len(reply), dim)
	}

	vector := make([]float32, dim)
	for i, component := range reply {
		value, err := parseFloatReplyValue(component, key, member)
		if err != nil {
			return nil, err
		}
		vector[i] = float32(value)
	}

	return vector, nil
}

// Redis replies with doubles under RESP3 and with bulk strings under RESP2.
func parseFloatReplyValue(value interface{}, key, member string) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse component '%s' in the VEMB reply for '%s' in vector set '%s': %w", v, member, key, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unexpected component '%v' of type %T in the VEMB reply for '%s' in vector set '%s'", value, value, member, key)
	}
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
