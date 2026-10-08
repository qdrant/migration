package commons

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/qdrant/go-client/qdrant"
)

func PrepareOffsetsCollection(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client) error {
	migrationOffsetCollectionExists, err := targetClient.CollectionExists(ctx, migrationOffsetsCollectionName)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if migrationOffsetCollectionExists {
		return nil
	}
	return targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: migrationOffsetsCollectionName,
		VectorsConfig:  qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{}),
	})
}

func DeleteOffsetsCollection(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client) error {
	migrationOffsetCollectionExists, err := targetClient.CollectionExists(ctx, migrationOffsetsCollectionName)
	if err != nil {
		return fmt.Errorf("failed to check if collection exists: %w", err)
	}
	if !migrationOffsetCollectionExists {
		fmt.Printf("Collection %s does not exist, nothing to delete\n", migrationOffsetsCollectionName)
		return nil
	}
	return targetClient.DeleteCollection(ctx, migrationOffsetsCollectionName)
}

func GetStartOffset(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client, sourceCollection string) (*qdrant.PointId, uint64, error) {
	point, err := getOffsetPoint(ctx, migrationOffsetsCollectionName, targetClient, sourceCollection)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get start offset point: %w", err)
	}
	if point == nil {
		return nil, 0, nil
	}
	offset, ok := point.Payload[sourceCollection+"_offset"]
	if !ok {
		return nil, 0, nil
	}
	offsetCount, ok := point.Payload[sourceCollection+"_offsetCount"]
	if !ok {
		return nil, 0, nil
	}

	offsetCountValue, ok := offsetCount.GetKind().(*qdrant.Value_IntegerValue)
	if !ok {
		return nil, 0, fmt.Errorf("failed to get offset count: invalid type")
	}

	offsetIntegerValue, ok := offset.GetKind().(*qdrant.Value_IntegerValue)
	if ok {
		return qdrant.NewIDNum(uint64(offsetIntegerValue.IntegerValue)), uint64(offsetCountValue.IntegerValue), nil
	}

	offsetStringValue, ok := offset.GetKind().(*qdrant.Value_StringValue)
	if ok {
		return qdrant.NewIDUUID(offsetStringValue.StringValue), uint64(offsetCountValue.IntegerValue), nil
	}

	return nil, 0, nil
}

func getOffsetIdAsValue(offset *qdrant.PointId) (interface{}, error) {
	switch pointID := offset.GetPointIdOptions().(type) {
	case *qdrant.PointId_Num:
		return pointID.Num, nil
	case *qdrant.PointId_Uuid:
		return pointID.Uuid, nil
	default:
		return nil, fmt.Errorf("unsupported offset type: %T", pointID)
	}
}

func StoreStartOffset(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client, sourceCollection string, offset *qdrant.PointId, offsetCount uint64) error {
	if offset == nil {
		return nil
	}
	offsetId, err := getOffsetIdAsValue(offset)
	if err != nil {
		return err
	}

	payload := qdrant.NewValueMap(map[string]any{
		sourceCollection + "_offset":       offsetId,
		sourceCollection + "_offsetCount":  offsetCount,
		sourceCollection + "_lastUpsertAt": time.Now().Format(time.RFC3339),
	})

	_, err = targetClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: migrationOffsetsCollectionName,
		Points: []*qdrant.PointStruct{
			{
				Id:      getOffsetPointId(sourceCollection),
				Payload: payload,
				Vectors: qdrant.NewVectorsMap(map[string]*qdrant.Vector{}),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to store offset: %w", err)
	}
	return nil
}

func getOffsetPoint(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client, sourceCollection string) (*qdrant.RetrievedPoint, error) {
	points, err := targetClient.Get(ctx, &qdrant.GetPoints{
		CollectionName: migrationOffsetsCollectionName,
		Ids:            []*qdrant.PointId{getOffsetPointId(sourceCollection)},
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get start offset: %w", err)
	}
	if len(points) == 0 {
		return nil, nil
	}

	return points[0], nil
}

func getOffsetPointId(sourceCollection string) *qdrant.PointId {
	deterministicUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(sourceCollection))

	return qdrant.NewIDUUID(deterministicUUID.String())
}

// EncodePointID serializes a point ID into a string that keeps its type ("n:<num>" or "u:<uuid>").
func EncodePointID(id *qdrant.PointId) (string, error) {
	switch v := id.GetPointIdOptions().(type) {
	case *qdrant.PointId_Num:
		return "n:" + strconv.FormatUint(v.Num, 10), nil
	case *qdrant.PointId_Uuid:
		return "u:" + v.Uuid, nil
	default:
		return "", fmt.Errorf("unsupported point id type: %T", v)
	}
}

// DecodePointID is the inverse of EncodePointID.
func DecodePointID(s string) (*qdrant.PointId, error) {
	switch {
	case strings.HasPrefix(s, "n:"):
		n, err := strconv.ParseUint(strings.TrimPrefix(s, "n:"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid numeric point id %q: %w", s, err)
		}
		return qdrant.NewIDNum(n), nil
	case strings.HasPrefix(s, "u:"):
		return qdrant.NewIDUUID(strings.TrimPrefix(s, "u:")), nil
	default:
		return nil, fmt.Errorf("invalid encoded point id %q", s)
	}
}

// BoundariesFingerprint returns a short, stable identifier of a set of range boundaries.
// It is used to namespace per-range offsets, so an offset saved for one set of boundaries
// can never be applied to a different set (which would silently skip points on resume).
func BoundariesFingerprint(ids []*qdrant.PointId) (string, error) {
	keys, err := encodePointIDs(ids)
	if err != nil {
		return "", err
	}
	return BoundaryKeysFingerprint(keys), nil
}

// BoundaryKeysFingerprint is BoundariesFingerprint for string keys.
func BoundaryKeysFingerprint(keys []string) string {
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

func encodePointIDs(ids []*qdrant.PointId) ([]string, error) {
	keys := make([]string, len(ids))
	for i, id := range ids {
		s, err := EncodePointID(id)
		if err != nil {
			return nil, err
		}
		keys[i] = s
	}
	return keys, nil
}

// StoreBoundaries persists the range boundaries used by a parallel migration under the given key.
func StoreBoundaries(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client, key string, ids []*qdrant.PointId) error {
	keys, err := encodePointIDs(ids)
	if err != nil {
		return err
	}
	return StoreBoundaryKeys(ctx, migrationOffsetsCollectionName, targetClient, key, keys)
}

// StoreBoundaryKeys is StoreBoundaries for string keys.
func StoreBoundaryKeys(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client, key string, keys []string) error {
	values := make([]any, len(keys))
	for i, k := range keys {
		values[i] = k
	}
	_, err := targetClient.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: migrationOffsetsCollectionName,
		Wait:           qdrant.PtrOf(true),
		Points: []*qdrant.PointStruct{
			{
				Id:      getOffsetPointId(key),
				Payload: qdrant.NewValueMap(map[string]any{key + "_boundaries": values}),
				Vectors: qdrant.NewVectorsMap(map[string]*qdrant.Vector{}),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to store range boundaries: %w", err)
	}
	return nil
}

// GetBoundaries loads range boundaries stored by StoreBoundaries. It returns nil if none were stored.
func GetBoundaries(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client, key string) ([]*qdrant.PointId, error) {
	keys, err := GetBoundaryKeys(ctx, migrationOffsetsCollectionName, targetClient, key)
	if err != nil || keys == nil {
		return nil, err
	}
	ids := make([]*qdrant.PointId, 0, len(keys))
	for _, k := range keys {
		id, err := DecodePointID(k)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// GetBoundaryKeys loads range boundaries stored by StoreBoundaryKeys. It returns nil if none were stored.
func GetBoundaryKeys(ctx context.Context, migrationOffsetsCollectionName string, targetClient *qdrant.Client, key string) ([]string, error) {
	point, err := getOffsetPoint(ctx, migrationOffsetsCollectionName, targetClient, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get range boundaries: %w", err)
	}
	if point == nil {
		return nil, nil
	}
	value, ok := point.Payload[key+"_boundaries"]
	if !ok {
		return nil, nil
	}
	list := value.GetListValue()
	if list == nil {
		return nil, fmt.Errorf("stored range boundaries have invalid type")
	}
	keys := make([]string, 0, len(list.GetValues()))
	for _, v := range list.GetValues() {
		keys = append(keys, v.GetStringValue())
	}
	return keys, nil
}
