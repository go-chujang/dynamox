package dynamox

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type (
	BatchWriteCallbackFn     func(*dynamodb.BatchWriteItemOutput) error
	BatchGetCallbackFn       func(*dynamodb.BatchGetItemOutput) error
	TransactionGetCallbackFn func(*dynamodb.TransactGetItemsOutput) error
)

var (
	_ apiSpec           = (*Client)(nil)
	_ crudSpec          = (*cruder)(nil)
	_ controlPlane      = (*Client)(nil)
	_ marshal_unmarshal = nil
)

// just spec, not implements guide
type (
	apiSpec interface {
		Get(query *CtxQuery, output any) error
		Query(query *CtxQuery, output any) (count int32, lastEvaluatedKey PaginationKey, err error)
		Scan(query *CtxQuery, output any) (count int32, lastEvaluatedKey PaginationKey, err error)
		Put(query *CtxQuery, outputOps ...any) error
		Update(query *CtxQuery, outputOps ...any) error
		Delete(query *CtxQuery, outputOps ...any) error

		BatchWrite(query *CtxQuery) (map[string][]types.WriteRequest, error)
		BatchWriteWithCallBack(query *CtxQuery, callback BatchWriteCallbackFn) error
		BatchGet(query *CtxQuery) (map[string][]map[string]types.AttributeValue, error)
		BatchGetWithCallBack(query *CtxQuery, callback BatchGetCallbackFn) error

		TransactionWrite(query *CtxQuery) error
		TransactionGet(query *CtxQuery) ([]map[string]types.AttributeValue, error)
		TransactionGetWithCallBack(query *CtxQuery, callback TransactionGetCallbackFn) error
	}

	crudSpec interface {
		Exist(ctx context.Context, keyedItem KeyedItem, withSk bool, consistent ...bool) (bool, error)
		Create(ctx context.Context, keyedItem KeyedItem, strictPk bool) error
		Read(ctx context.Context, keyedItem KeyedItem, consistent ...bool) error
		Update(ctx context.Context, keyedItem KeyedItem, strictPk bool) error
		Delete(ctx context.Context, keyedItem KeyedItem) error
		DeleteSoft(ctx context.Context, keyedItem KeyedItem) error
	}

	controlPlane interface {
		TableExists(ctx context.Context, name string) (bool, error)
		TableList(ctx context.Context, limitOps ...int32) (list []string, err error)
		TableApproximateItemCount(ctx context.Context, name string) (int64, error)
	}

	marshal_unmarshal interface {
		PreMarshal(item KeyedItem, skipSaveSK ...bool) error
		Prepare(item KeyedItem, skipSaveSK ...bool) error
		MarshalMap(item KeyedItem, skipSaveSK ...bool) (map[string]types.AttributeValue, error)
		MarshalMapByAny(item any, skipSaveSK ...bool) (map[string]types.AttributeValue, error)
		MarshalMapOnlyKey(item KeyedItem, skipSaveSK ...bool) (map[string]types.AttributeValue, error)

		PostUnmarshal(out KeyedItem)
		UnmarshalMap(m map[string]types.AttributeValue, out KeyedItem) error
		UnmarshalMapByAny(m map[string]types.AttributeValue, out any) error
		UnmarshalListOfMapsByAny(l []map[string]types.AttributeValue, out any) error
		// with generic
		// UnmarshalListOfMaps[T KeyedItem](l []map[string]types.AttributeValue, out *[]T) error
	}
)
