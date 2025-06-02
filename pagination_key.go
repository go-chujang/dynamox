package dynamox

import (
	"encoding/base64"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PaginationKey map[string]types.AttributeValue

var (
	_ json.Marshaler   = (*PaginationKey)(nil)
	_ json.Unmarshaler = (*PaginationKey)(nil)
)

func (pgk *PaginationKey) Import(token string) (PaginationKey, error) {
	if token == "" {
		return nil, nil
	}
	b, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	var wrapper map[string]map[string]string
	if err := json.Unmarshal(b, &wrapper); err != nil {
		return nil, err
	}
	out := make(map[string]types.AttributeValue, len(wrapper))
	for k, m := range wrapper {
		switch {
		case m["S"] != "":
			out[k] = &types.AttributeValueMemberS{Value: m["S"]}
		case m["N"] != "":
			out[k] = &types.AttributeValueMemberN{Value: m["N"]}
		case m["B"] != "":
			data, err := base64.StdEncoding.DecodeString(m["B"])
			if err != nil {
				return nil, err
			}
			out[k] = &types.AttributeValueMemberB{Value: data}
		default:
			return nil, ErrUnsupportedAttrValueTypeForKey
		}
	}
	return out, nil
}

func (pgk PaginationKey) MarshalJSON() ([]byte, error) {
	if len(pgk) == 0 {
		return json.Marshal("")
	}
	wrapper := make(map[string]map[string]string, len(pgk))
	for k, av := range pgk {
		m := make(map[string]string, 1)
		switch v := av.(type) {
		case *types.AttributeValueMemberS:
			m["S"] = v.Value
		case *types.AttributeValueMemberN:
			m["N"] = v.Value
		case *types.AttributeValueMemberB:
			m["B"] = base64.StdEncoding.EncodeToString(v.Value)
		default:
			return nil, ErrUnsupportedAttrValueTypeForKey
		}
		wrapper[k] = m
	}
	b, err := json.Marshal(wrapper)
	if err != nil {
		return nil, err
	}
	token := base64.URLEncoding.EncodeToString(b)
	return json.Marshal(token)
}

func (pgk *PaginationKey) UnmarshalJSON(data []byte) error {
	var token string
	if err := json.Unmarshal(data, &token); err != nil {
		return err
	}
	out, err := pgk.Import(token)
	if err != nil {
		return err
	}
	*pgk = out
	return nil
}
