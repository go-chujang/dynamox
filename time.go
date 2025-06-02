package dynamox

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type (
	TimeDefaultNow int64
	Time           int64 // omitempty without pointer
)

func TimeNow() Time                     { return Time(newTimestampFn()) }
func TimeByTime(t time.Time) Time       { return Time(t.UnixMilli()) }
func (dt Time) Int64() int64            { return int64(dt) }
func (dt Time) Time() time.Time         { return parseTimestampFn(dt.Int64()) }
func (dt Time) Duration() time.Duration { return time.Duration(dt) }

var (
	_ attributevalue.Marshaler   = (*TimeDefaultNow)(nil)
	_ json.Marshaler             = (*Time)(nil)
	_ json.Unmarshaler           = (*Time)(nil)
	_ attributevalue.Marshaler   = (*Time)(nil)
	_ attributevalue.Unmarshaler = (*Time)(nil)
)

func (dtn TimeDefaultNow) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	if dtn == 0 {
		dtn = TimeDefaultNow(TimeNow())
	}
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(dtn), 10)}, nil
}

func (dt Time) MarshalJSON() ([]byte, error) {
	if dt == 0 {
		return []byte("null"), nil
	}
	return []byte(strconv.FormatInt(int64(dt), 10)), nil
}

func (dt *Time) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		*dt = 0
		return nil
	}
	s := string(data)
	if s == "null" {
		*dt = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		*dt = Time(v)
	}
	return err
}

func (dt Time) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	if dt == 0 {
		return nil, nil
	}
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(dt), 10)}, nil
}

func (dt *Time) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	if av == nil {
		return nil
	}
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return errors.New("expected N attribute")
	}
	v, err := strconv.ParseInt(n.Value, 10, 64)
	if err == nil {
		*dt = Time(v)
	}
	return err
}
