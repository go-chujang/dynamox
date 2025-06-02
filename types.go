package dynamox

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type (
	ID    string
	IDSeq string
)

var (
	_ attributevalue.Marshaler = (*ID)(nil)
	_ attributevalue.Marshaler = (*IDSeq)(nil)
)

func NewID() ID              { return ID(uuidV4()) }
func (id ID) String() string { return string(id) }
func (id ID) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	if len(id) == 0 {
		id = NewID()
	}
	return &types.AttributeValueMemberS{Value: string(id)}, nil
}

func NewIDSeq() IDSeq           { return IDSeq(ulidStr()) }
func (id IDSeq) String() string { return string(id) }
func (id IDSeq) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	if len(id) == 0 {
		id = NewIDSeq()
	}
	return &types.AttributeValueMemberS{Value: string(id)}, nil
}

type SortKeyPrefix string

func (dsp SortKeyPrefix) String() string {
	return string(dsp)
}

func (dsp SortKeyPrefix) Composite(srcs ...string) string {
	return compositeKey(append([]string{dsp.String()}, srcs...)...)
}
