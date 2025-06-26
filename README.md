# Quickstart

## Basic API
more example: **example/api_test.go**
```go
cli = dynamox.NewClient(awsConfig)

item, _ := attributevalue.MarshalMap(v)
cli.Put(dynamox.NewCtxQuery().SetTable(table).SetItem(item))
cli.Get(dynamox.NewCtxQuery().SetTable(table).SetKey(key))

var out someStruct
expr, _ := expression.NewBuilder().WithKeyCondition(keyCond).Build()
cli.Query(dynamox.NewCtxQuery().SetTable(table).ExprQuery(expr), &out)
```

## CRUD API
example details: **example/keyeditem_test.go**
![CustomerBookmark](https://github.com/user-attachments/assets/422b3986-acc2-47cd-a5b1-3aa52482f2d5)

```go
// AWS NoSQL Workbench / Bookmarks Data Model
var (
	_ dynamox.KeyBase   = (*PartitionBase)(nil)
	_ dynamox.KeyedItem = (*Profile)(nil)
	_ dynamox.KeyedItem = (*Bookmark)(nil)
)

base := PartitionBase{CustomerId: "123"}
items = []dynamox.KeyedItem{
    &Profile{PartitionBase: base, ...}
    &Bookmark{PartitionBase: base, URL: "https://aws.amazon.com", ...}
    &Bookmark{PartitionBase: base, URL: "https://console.aws.amazon.com", ...}
}

for _, v := range items {
    cli.Cruder().Create(context.Background(), v, true)
}

profile := Profile{PartitionBase: base}
cli.Cruder().Read(context.Background(), &profile)
```



## api_spec.go
**just spec, not implements guide**
```go
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
)
```
