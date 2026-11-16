package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/qdrant/go-client/qdrant"
)

func TestParseFilter(t *testing.T) {
	uuid := "0f4a6de3-c18b-5de3-992b-5eb7f5c52b1a"
	tests := []struct {
		name     string
		filter   string
		expected *qdrant.Filter
	}{
		{
			name:     "null values are the same as no values",
			filter:   `{"must": [{"key": "n", "match": {"value": 1}, "range": null, "is_empty": null}, {"is_null": {"key": "b"}, "is_empty": null}], "should": null}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatchInt("n", 1), qdrant.NewIsNull("b")}},
		},
		{
			name:     "match a keyword",
			filter:   `{"must": [{"key": "tenant", "match": {"value": "acme"}}]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatchKeyword("tenant", "acme")}},
		},
		{
			name:     "a single condition instead of a list",
			filter:   `{"must": {"key": "tenant", "match": {"value": "acme"}}}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatchKeyword("tenant", "acme")}},
		},
		{
			name:   "match an integer, a large integer, and a boolean",
			filter: `{"should": [{"key": "a", "match": {"value": 7}}, {"key": "b", "match": {"value": 9007199254740993}}, {"key": "c", "match": {"value": false}}]}`,
			expected: &qdrant.Filter{Should: []*qdrant.Condition{
				qdrant.NewMatchInt("a", 7), qdrant.NewMatchInt("b", 9007199254740993), qdrant.NewMatchBool("c", false),
			}},
		},
		{
			name: "match text kinds",
			filter: `{"must": [{"key": "t", "match": {"text": "x y"}}, {"key": "t", "match": {"text_any": "x y"}}, {"key": "t", "match": {"phrase": "x y"}},
				{"key": "k", "match": {"prefix": "ab"}}, {"key": "k", "match": {"substring": "bc"}}]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{
				qdrant.NewMatchText("t", "x y"), qdrant.NewMatchTextAny("t", "x y"), qdrant.NewMatchPhrase("t", "x y"),
				qdrant.NewMatchPrefix("k", "ab"), qdrant.NewMatchSubstring("k", "bc"),
			}},
		},
		{
			name: "match any and except",
			filter: `{"must": [{"key": "k", "match": {"any": ["a", "b"]}}, {"key": "n", "match": {"any": [1, 2]}}],
				"must_not": [{"key": "k", "match": {"except": ["c"]}}, {"key": "n", "match": {"except": [3]}}]}`,
			expected: &qdrant.Filter{
				Must:    []*qdrant.Condition{qdrant.NewMatchKeywords("k", "a", "b"), qdrant.NewMatchInts("n", 1, 2)},
				MustNot: []*qdrant.Condition{qdrant.NewMatchExcept("k", "c"), qdrant.NewMatchExceptInts("n", 3)},
			},
		},
		{
			name:     "numeric range",
			filter:   `{"must": [{"key": "price", "range": {"gte": 1.5, "lt": 10}}]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewRange("price", &qdrant.Range{Gte: qdrant.PtrOf(1.5), Lt: qdrant.PtrOf(10.0)})}},
		},
		{
			name:   "datetime range",
			filter: `{"must": [{"key": "at", "range": {"gt": "2024-01-02T03:04:05Z", "lte": "2024-02-01"}}]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewDatetimeRange("at", &qdrant.DatetimeRange{
				Gt:  timestamppb.New(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)),
				Lte: timestamppb.New(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
			})}},
		},
		{
			name: "geo conditions",
			filter: `{"must": [
				{"key": "loc", "geo_bounding_box": {"top_left": {"lon": 1, "lat": 4}, "bottom_right": {"lon": 3, "lat": 2}}},
				{"key": "loc", "geo_radius": {"center": {"lon": 1, "lat": 2}, "radius": 1000}},
				{"key": "loc", "geo_polygon": {"exterior": {"points": [{"lon": 0, "lat": 0}, {"lon": 1, "lat": 0}, {"lon": 0, "lat": 1}, {"lon": 0, "lat": 0}]},
					"interiors": [{"points": [{"lon": 0.1, "lat": 0.1}, {"lon": 0.2, "lat": 0.1}, {"lon": 0.1, "lat": 0.2}, {"lon": 0.1, "lat": 0.1}]}]}}
			]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{
				qdrant.NewGeoBoundingBox("loc", 4, 1, 2, 3),
				qdrant.NewGeoRadius("loc", 2, 1, 1000),
				qdrant.NewGeoPolygon("loc",
					&qdrant.GeoLineString{Points: []*qdrant.GeoPoint{{Lon: 0, Lat: 0}, {Lon: 1, Lat: 0}, {Lon: 0, Lat: 1}, {Lon: 0, Lat: 0}}},
					&qdrant.GeoLineString{Points: []*qdrant.GeoPoint{{Lon: 0.1, Lat: 0.1}, {Lon: 0.2, Lat: 0.1}, {Lon: 0.1, Lat: 0.2}, {Lon: 0.1, Lat: 0.1}}}),
			}},
		},
		{
			name:     "values count",
			filter:   `{"must": [{"key": "tags", "values_count": {"gt": 1, "lte": 3}}]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewValuesCount("tags", &qdrant.ValuesCount{Gt: qdrant.PtrOf(uint64(1)), Lte: qdrant.PtrOf(uint64(3))})}},
		},
		{
			name:   "is_empty and is_null, as conditions and as field options",
			filter: `{"must": [{"is_empty": {"key": "a"}}, {"is_null": {"key": "b"}}, {"key": "c", "is_empty": false}, {"key": "d", "is_null": true}]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{
				qdrant.NewIsEmpty("a"), qdrant.NewIsNull("b"),
				{ConditionOneOf: &qdrant.Condition_Field{Field: &qdrant.FieldCondition{Key: "c", IsEmpty: qdrant.PtrOf(false)}}},
				{ConditionOneOf: &qdrant.Condition_Field{Field: &qdrant.FieldCondition{Key: "d", IsNull: qdrant.PtrOf(true)}}},
			}},
		},
		{
			name:   "point IDs, vectors, and slices",
			filter: `{"must": [{"has_id": [1, 18446744073709551615, "` + uuid + `"]}, {"has_vector": "image"}, {"slice": {"total": 4, "index": 1}}]}`,
			expected: &qdrant.Filter{Must: []*qdrant.Condition{
				qdrant.NewHasID(qdrant.NewIDNum(1), qdrant.NewIDNum(18446744073709551615), qdrant.NewIDUUID(uuid)),
				qdrant.NewHasVector("image"),
				{ConditionOneOf: &qdrant.Condition_Slice{Slice: &qdrant.SliceCondition{Total: 4, Index: 1}}},
			}},
		},
		{
			name: "nested object, nested filter, and min_should",
			filter: `{"must": [{"nested": {"key": "items", "filter": {"must": [{"key": "sku", "match": {"value": "x"}}]}}},
				{"should": [{"key": "a", "match": {"value": 1}}], "must_not": [{"key": "b", "match": {"value": 2}}]}],
				"min_should": {"conditions": [{"key": "c", "match": {"value": 3}}, {"key": "d", "match": {"value": 4}}], "min_count": 1}}`,
			expected: &qdrant.Filter{
				Must: []*qdrant.Condition{
					qdrant.NewNestedFilter("items", &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatchKeyword("sku", "x")}}),
					qdrant.NewFilterAsCondition(&qdrant.Filter{
						Should:  []*qdrant.Condition{qdrant.NewMatchInt("a", 1)},
						MustNot: []*qdrant.Condition{qdrant.NewMatchInt("b", 2)},
					}),
				},
				MinShould: &qdrant.MinShould{Conditions: []*qdrant.Condition{qdrant.NewMatchInt("c", 3), qdrant.NewMatchInt("d", 4)}, MinCount: 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := parseFilter(tt.filter)
			require.NoError(t, err)
			require.True(t, proto.Equal(tt.expected, filter), "got: %v", filter)
		})
	}
}

func TestParseFilterErrors(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		err    string
	}{
		{"not JSON", `{"must": [`, "invalid filter"},
		{"data after the filter", `{} {}`, "unexpected data"},
		{"unknown key", `{"must": [{"key": "a", "matches": {"value": 1}}]}`, `unknown field "matches"`},
		{"two kinds of condition", `{"must": [{"is_empty": {"key": "a"}, "has_vector": "v"}]}`, "exactly one of"},
		{"field condition with another kind", `{"must": [{"key": "a", "has_vector": "v", "match": {"value": 1}}]}`, "another kind of condition"},
		{"field condition without a condition", `{"must": [{"key": "a"}]}`, "has no condition"},
		{"is_empty of a field is not a boolean", `{"must": [{"key": "a", "is_empty": {"key": "a"}}]}`, "must be true or false"},
		{"two match kinds", `{"must": [{"key": "a", "match": {"value": 1, "text": "x"}}]}`, "exactly one of"},
		{"fractional match value", `{"must": [{"key": "a", "match": {"value": 1.5}}]}`, "must be an integer"},
		{"mixed match any", `{"must": [{"key": "a", "match": {"any": ["x", 1]}}]}`, "only strings or only integers"},
		{"mixed range bounds", `{"must": [{"key": "a", "range": {"gt": 1, "lt": "2024-01-01"}}]}`, "all numbers or all datetime strings"},
		{"bad datetime", `{"must": [{"key": "a", "range": {"gt": "yesterday"}}]}`, "is not a datetime"},
		{"negative point ID", `{"must": [{"has_id": [-1]}]}`, "unsigned integer or a UUID"},
		{"null range alone", `{"must": [{"key": "a", "range": null}]}`, "has no condition"},
		{"null is_empty alone", `{"must": [{"is_empty": null}]}`, "exactly one of"},
		{"nested condition without a filter", `{"must": [{"nested": {"key": "items"}}]}`, "has no filter"},
		{"point ID that is not a UUID", `{"must": [{"has_id": ["abc"]}]}`, "unsigned integer or a UUID"},
		{"error in a nested filter", `{"must": [{"nested": {"key": "items", "filter": {"must": [{"key": "a"}]}}}]}`, "has no condition"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseFilter(tt.filter)
			require.ErrorContains(t, err, tt.err)
		})
	}
}
