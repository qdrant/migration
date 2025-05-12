package commons

import (
	"context"
	"fmt"
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
		return nil, 0, fmt.Errorf("failed to get offset count: %w", err)
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
