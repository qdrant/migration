package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/qdrant/go-client/qdrant"
)

// restFilter is a filter in the JSON format of the Qdrant REST API.
// See https://qdrant.tech/documentation/concepts/filtering/.
type restFilter struct {
	Must      restConditions `json:"must"`
	Should    restConditions `json:"should"`
	MustNot   restConditions `json:"must_not"`
	MinShould *struct {
		Conditions restConditions `json:"conditions"`
		MinCount   uint64         `json:"min_count"`
	} `json:"min_should"`
}

// restCondition is one condition of a restFilter. Exactly one kind of condition is set.
// The geo, values count, and slice conditions use the gRPC types, because their JSON names are the same.
type restCondition struct {
	restFilter

	Key            *string                `json:"key"`
	Match          *restMatch             `json:"match"`
	Range          json.RawMessage        `json:"range"`
	GeoBoundingBox *qdrant.GeoBoundingBox `json:"geo_bounding_box"`
	GeoRadius      *qdrant.GeoRadius      `json:"geo_radius"`
	GeoPolygon     *qdrant.GeoPolygon     `json:"geo_polygon"`
	ValuesCount    *qdrant.ValuesCount    `json:"values_count"`
	// IsEmpty and IsNull are a boolean in a field condition, and {"key": "..."} on their own.
	IsEmpty   json.RawMessage        `json:"is_empty"`
	IsNull    json.RawMessage        `json:"is_null"`
	HasID     []any                  `json:"has_id"`
	HasVector *string                `json:"has_vector"`
	Nested    *restNested            `json:"nested"`
	Slice     *qdrant.SliceCondition `json:"slice"`
}

type restNested struct {
	Key    string      `json:"key"`
	Filter *restFilter `json:"filter"`
}

type restMatch struct {
	Value     any     `json:"value"`
	Text      *string `json:"text"`
	TextAny   *string `json:"text_any"`
	Phrase    *string `json:"phrase"`
	Prefix    *string `json:"prefix"`
	Substring *string `json:"substring"`
	Any       []any   `json:"any"`
	Except    []any   `json:"except"`
}

// restConditions is a list of conditions, or a single condition.
type restConditions []restCondition

func (c *restConditions) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	if bytes.HasPrefix(data, []byte("[")) {
		return decodeJSON(data, (*[]restCondition)(c))
	}
	var condition restCondition
	if err := decodeJSON(data, &condition); err != nil {
		return err
	}
	*c = restConditions{condition}
	return nil
}

// hasValue reports whether a JSON value is present and not null. As in the REST API, a null value is the same as no value.
func hasValue(raw json.RawMessage) bool {
	return raw != nil && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// decodeJSON rejects unknown keys and keeps numbers as json.Number, so that large integers keep their precision.
func decodeJSON(data []byte, v any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("unexpected data after the filter")
	}
	return nil
}

// parseFilter converts a filter in the JSON format of the Qdrant REST API into a filter for the gRPC API.
func parseFilter(data string) (*qdrant.Filter, error) {
	var rest restFilter
	if err := decodeJSON([]byte(data), &rest); err != nil {
		return nil, fmt.Errorf("invalid filter: %w", err)
	}
	filter, err := rest.toGRPC()
	if err != nil {
		return nil, fmt.Errorf("invalid filter: %w", err)
	}
	return filter, nil
}

func (f *restFilter) toGRPC() (*qdrant.Filter, error) {
	filter := &qdrant.Filter{}
	var err error
	if filter.Must, err = f.Must.toGRPC(); err != nil {
		return nil, err
	}
	if filter.Should, err = f.Should.toGRPC(); err != nil {
		return nil, err
	}
	if filter.MustNot, err = f.MustNot.toGRPC(); err != nil {
		return nil, err
	}
	if f.MinShould != nil {
		conditions, err := f.MinShould.Conditions.toGRPC()
		if err != nil {
			return nil, err
		}
		filter.MinShould = &qdrant.MinShould{Conditions: conditions, MinCount: f.MinShould.MinCount}
	}
	return filter, nil
}

func (f *restFilter) isSet() bool {
	return f.Must != nil || f.Should != nil || f.MustNot != nil || f.MinShould != nil
}

func (c restConditions) toGRPC() ([]*qdrant.Condition, error) {
	var conditions []*qdrant.Condition
	for _, rest := range c {
		condition, err := rest.toGRPC()
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, condition)
	}
	return conditions, nil
}

func (c *restCondition) toGRPC() (*qdrant.Condition, error) {
	if c.Key != nil {
		return c.fieldCondition()
	}
	kinds := 0
	for _, set := range []bool{hasValue(c.IsEmpty), hasValue(c.IsNull), c.HasID != nil, c.HasVector != nil, c.Nested != nil, c.Slice != nil, c.isSet()} {
		if set {
			kinds++
		}
	}
	if kinds != 1 {
		return nil, errors.New("a condition must be exactly one of: a field condition with a key, is_empty, is_null, has_id, has_vector, nested, slice, or a filter")
	}

	switch {
	case hasValue(c.IsEmpty), hasValue(c.IsNull):
		var field struct {
			Key string `json:"key"`
		}
		raw := c.IsNull
		if hasValue(c.IsEmpty) {
			raw = c.IsEmpty
		}
		if err := decodeJSON(raw, &field); err != nil {
			return nil, fmt.Errorf("is_empty and is_null take {\"key\": \"...\"}: %w", err)
		}
		if hasValue(c.IsEmpty) {
			return qdrant.NewIsEmpty(field.Key), nil
		}
		return qdrant.NewIsNull(field.Key), nil
	case c.HasID != nil:
		ids := make([]*qdrant.PointId, 0, len(c.HasID))
		for _, id := range c.HasID {
			pointID, err := pointIDFromJSON(id)
			if err != nil {
				return nil, err
			}
			ids = append(ids, pointID)
		}
		return qdrant.NewHasID(ids...), nil
	case c.HasVector != nil:
		return qdrant.NewHasVector(*c.HasVector), nil
	case c.Nested != nil:
		if c.Nested.Filter == nil {
			return nil, fmt.Errorf("the nested condition of %q has no filter", c.Nested.Key)
		}
		filter, err := c.Nested.Filter.toGRPC()
		if err != nil {
			return nil, err
		}
		return qdrant.NewNestedFilter(c.Nested.Key, filter), nil
	case c.Slice != nil:
		return &qdrant.Condition{ConditionOneOf: &qdrant.Condition_Slice{Slice: c.Slice}}, nil
	}
	filter, err := c.restFilter.toGRPC()
	if err != nil {
		return nil, err
	}
	return qdrant.NewFilterAsCondition(filter), nil
}

func (c *restCondition) fieldCondition() (*qdrant.Condition, error) {
	if c.HasID != nil || c.HasVector != nil || c.Nested != nil || c.Slice != nil || c.isSet() {
		return nil, fmt.Errorf("the field condition of %q has keys of another kind of condition", *c.Key)
	}
	field := &qdrant.FieldCondition{
		Key:            *c.Key,
		GeoBoundingBox: c.GeoBoundingBox,
		GeoRadius:      c.GeoRadius,
		GeoPolygon:     c.GeoPolygon,
		ValuesCount:    c.ValuesCount,
	}
	var err error
	if c.Match != nil {
		if field.Match, err = c.Match.toGRPC(); err != nil {
			return nil, fmt.Errorf("match of %q: %w", *c.Key, err)
		}
	}
	if hasValue(c.Range) {
		if field.Range, field.DatetimeRange, err = rangeFromJSON(c.Range); err != nil {
			return nil, fmt.Errorf("range of %q: %w", *c.Key, err)
		}
	}
	for raw, target := range map[*json.RawMessage]**bool{&c.IsEmpty: &field.IsEmpty, &c.IsNull: &field.IsNull} {
		if hasValue(*raw) {
			if err := decodeJSON(*raw, target); err != nil {
				return nil, fmt.Errorf("is_empty and is_null of %q must be true or false", *c.Key)
			}
		}
	}
	if field.Match == nil && field.Range == nil && field.DatetimeRange == nil && field.GeoBoundingBox == nil && field.GeoRadius == nil &&
		field.GeoPolygon == nil && field.ValuesCount == nil && field.IsEmpty == nil && field.IsNull == nil {
		return nil, fmt.Errorf("the field condition of %q has no condition", *c.Key)
	}
	return &qdrant.Condition{ConditionOneOf: &qdrant.Condition_Field{Field: field}}, nil
}

func (m *restMatch) toGRPC() (*qdrant.Match, error) {
	var matches []*qdrant.Match
	if m.Value != nil {
		switch value := m.Value.(type) {
		case string:
			matches = append(matches, &qdrant.Match{MatchValue: &qdrant.Match_Keyword{Keyword: value}})
		case bool:
			matches = append(matches, &qdrant.Match{MatchValue: &qdrant.Match_Boolean{Boolean: value}})
		case json.Number:
			integer, err := value.Int64()
			if err != nil {
				return nil, fmt.Errorf("value %s must be an integer", value)
			}
			matches = append(matches, &qdrant.Match{MatchValue: &qdrant.Match_Integer{Integer: integer}})
		default:
			return nil, errors.New("value must be a string, an integer, or a boolean")
		}
	}
	for text, match := range map[*string]func(string) *qdrant.Match{
		m.Text:      func(s string) *qdrant.Match { return &qdrant.Match{MatchValue: &qdrant.Match_Text{Text: s}} },
		m.TextAny:   func(s string) *qdrant.Match { return &qdrant.Match{MatchValue: &qdrant.Match_TextAny{TextAny: s}} },
		m.Phrase:    func(s string) *qdrant.Match { return &qdrant.Match{MatchValue: &qdrant.Match_Phrase{Phrase: s}} },
		m.Prefix:    func(s string) *qdrant.Match { return &qdrant.Match{MatchValue: &qdrant.Match_Prefix{Prefix: s}} },
		m.Substring: func(s string) *qdrant.Match { return &qdrant.Match{MatchValue: &qdrant.Match_Substring{Substring: s}} },
	} {
		if text != nil {
			matches = append(matches, match(*text))
		}
	}
	if m.Any != nil {
		keywords, integers, err := anyVariants(m.Any)
		if err != nil {
			return nil, fmt.Errorf("any: %w", err)
		}
		if integers != nil {
			matches = append(matches, &qdrant.Match{MatchValue: &qdrant.Match_Integers{Integers: &qdrant.RepeatedIntegers{Integers: integers}}})
		} else {
			matches = append(matches, &qdrant.Match{MatchValue: &qdrant.Match_Keywords{Keywords: &qdrant.RepeatedStrings{Strings: keywords}}})
		}
	}
	if m.Except != nil {
		keywords, integers, err := anyVariants(m.Except)
		if err != nil {
			return nil, fmt.Errorf("except: %w", err)
		}
		if integers != nil {
			matches = append(matches, &qdrant.Match{MatchValue: &qdrant.Match_ExceptIntegers{ExceptIntegers: &qdrant.RepeatedIntegers{Integers: integers}}})
		} else {
			matches = append(matches, &qdrant.Match{MatchValue: &qdrant.Match_ExceptKeywords{ExceptKeywords: &qdrant.RepeatedStrings{Strings: keywords}}})
		}
	}
	if len(matches) != 1 {
		return nil, errors.New("a match must have exactly one of: value, text, text_any, phrase, prefix, substring, any, except")
	}
	return matches[0], nil
}

// anyVariants reads a list of strings or a list of integers. An empty list is a list of strings.
func anyVariants(values []any) ([]string, []int64, error) {
	keywords := []string{}
	var integers []int64
	for _, value := range values {
		switch v := value.(type) {
		case string:
			keywords = append(keywords, v)
		case json.Number:
			integer, err := v.Int64()
			if err != nil {
				return nil, nil, fmt.Errorf("%s must be an integer", v)
			}
			integers = append(integers, integer)
		}
	}
	if len(keywords)+len(integers) != len(values) || (len(keywords) > 0 && len(integers) > 0) {
		return nil, nil, errors.New("the list must hold only strings or only integers")
	}
	return keywords, integers, nil
}

// rangeFromJSON reads a numeric range, or a datetime range if its bounds are strings.
func rangeFromJSON(raw json.RawMessage) (*qdrant.Range, *qdrant.DatetimeRange, error) {
	numbers := &qdrant.Range{}
	if err := decodeJSON(raw, numbers); err == nil {
		return numbers, nil, nil
	}
	var datetimes struct {
		Lt, Gt, Gte, Lte *string
	}
	if err := decodeJSON(raw, &datetimes); err != nil {
		return nil, nil, errors.New("the bounds lt, gt, gte, and lte must be all numbers or all datetime strings")
	}
	datetimeRange := &qdrant.DatetimeRange{}
	for text, target := range map[*string]**timestamppb.Timestamp{
		datetimes.Lt: &datetimeRange.Lt, datetimes.Gt: &datetimeRange.Gt, datetimes.Gte: &datetimeRange.Gte, datetimes.Lte: &datetimeRange.Lte,
	} {
		if text == nil {
			continue
		}
		timestamp, err := parseDatetime(*text)
		if err != nil {
			return nil, nil, err
		}
		*target = timestamppb.New(timestamp)
	}
	return nil, datetimeRange, nil
}

// datetimeLayouts are the datetime formats that Qdrant accepts. A datetime without a time zone is in UTC.
var datetimeLayouts = []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999", "2006-01-02T15:04", "2006-01-02 15:04", "2006-01-02"}

func parseDatetime(text string) (time.Time, error) {
	for _, layout := range datetimeLayouts {
		if t, err := time.Parse(layout, text); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("%q is not a datetime, use RFC 3339, for example 2024-01-02T03:04:05Z", text)
}

// pointIDFromJSON reads a point ID: an unsigned integer or a UUID.
func pointIDFromJSON(value any) (*qdrant.PointId, error) {
	switch v := value.(type) {
	case string:
		if _, err := uuid.Parse(v); err == nil {
			return qdrant.NewIDUUID(v), nil
		}
	case json.Number:
		if id, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
			return qdrant.NewIDNum(id), nil
		}
	}
	return nil, fmt.Errorf("point ID %v must be an unsigned integer or a UUID", value)
}
