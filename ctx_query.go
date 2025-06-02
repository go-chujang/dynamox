package dynamox

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type CtxQuery struct {
	insufficient      bool
	insufficientCause error

	ctx context.Context

	tableName      string
	item           map[string]types.AttributeValue // [put]
	key            map[string]types.AttributeValue // [get, update] partitionKey or partitionkey+sortkey
	consistentRead bool                            // [get, query, scan] unsupported on GSI
	startKey       map[string]types.AttributeValue // [query, scan] for pagination, use LastEvaluatedKey
	index          *string                         // [query, scan]
	limit          *int32                          // [query, scan]
	orderByAsc     bool                            // [query] true: ASC, false: DESC
	selectAttr     types.Select                    // [query] if 'SPECIFIC_ATTRIBUTES' must be used with projectExpr

	condExpr       *string // [put, update]
	keyCondExpr    *string // [query]
	filterExpr     *string // [query]
	projectExpr    *string // [get]
	updateExpr     *string // [update]
	exprAttrNames  map[string]string
	exprAttrValues map[string]types.AttributeValue

	batchWriteItems map[string][]types.WriteRequest    // [batchWrite]  key is tableName
	batchGetItems   map[string]types.KeysAndAttributes // [batchGet]

	transactionWriteItems []types.TransactWriteItem
	clientRequestToken    *string
	transactionGetItems   []types.TransactGetItem

	returnValues                        types.ReturnValue                         // [put, update] none is default
	returnValuesOnConditionCheckFailure types.ReturnValuesOnConditionCheckFailure // [put, update] none is default

	keyCondBuilder *KeyCondBuilder // [query]
}

func NewCtxQuery(c ...context.Context) *CtxQuery {
	cq := &CtxQuery{}
	if len(c) > 0 {
		return cq.SetContext(c[0])
	}
	return cq.SetContext(context.Background())
}

func (cq *CtxQuery) Context() context.Context {
	if cq.ctx == nil {
		return context.Background()
	}
	return cq.ctx
}

func (cq *CtxQuery) SetContext(c context.Context) *CtxQuery {
	cq.ctx = c
	return cq
}

func (cq *CtxQuery) SetTable(t string) *CtxQuery {
	cq.tableName = t
	return cq
}

func (cq *CtxQuery) SetItem(m map[string]types.AttributeValue) *CtxQuery {
	cq.item = m
	return cq
}

func (cq *CtxQuery) SetKey(m map[string]types.AttributeValue) *CtxQuery {
	cq.key = m
	return cq
}

// it doesn't affect the batchget
func (cq *CtxQuery) SetConsistentRead(cr bool) *CtxQuery {
	cq.consistentRead = cr
	return cq
}

func (cq *CtxQuery) SetStartKey(m PaginationKey) *CtxQuery {
	cq.startKey = m
	return cq
}

func (cq *CtxQuery) SetIndex(idx string) *CtxQuery {
	if idx != "" {
		cq.index = &idx
	}
	return cq
}

func (cq *CtxQuery) SetLimit(l int32) *CtxQuery {
	if l > 0 {
		cq.limit = &l
	}
	return cq
}

func (cq *CtxQuery) SetOrderByAsc(asc bool) *CtxQuery {
	cq.orderByAsc = asc
	return cq
}

func (cq *CtxQuery) SetSelectAttr(s types.Select) *CtxQuery {
	cq.selectAttr = s
	return cq
}

func (cq *CtxQuery) SetCondExpr(expr *string) *CtxQuery {
	cq.condExpr = expr
	return cq
}

func (cq *CtxQuery) SetKeyCondExpr(expr *string) *CtxQuery {
	cq.keyCondExpr = expr
	return cq
}

func (cq *CtxQuery) SetFilterExpr(expr *string) *CtxQuery {
	cq.filterExpr = expr
	return cq
}

func (cq *CtxQuery) SetProjectExpr(expr *string) *CtxQuery {
	cq.projectExpr = expr
	return cq
}

func (cq *CtxQuery) SetUpdateExpr(expr *string) *CtxQuery {
	cq.updateExpr = expr
	return cq
}

func (cq *CtxQuery) SetExprAttrNames(names map[string]string) *CtxQuery {
	cq.exprAttrNames = names
	return cq
}

func (cq *CtxQuery) SetExprAttrValues(values map[string]types.AttributeValue) *CtxQuery {
	cq.exprAttrValues = values
	return cq
}

func (cq *CtxQuery) SetBatchWriteItems(items map[string][]types.WriteRequest) *CtxQuery {
	cq.batchWriteItems = items
	return cq
}

func (cq *CtxQuery) SetBatchGetItems(items map[string]types.KeysAndAttributes) *CtxQuery {
	cq.batchGetItems = items
	return cq
}

func (cq *CtxQuery) SetTransactionWriteItems(items []types.TransactWriteItem) *CtxQuery {
	cq.transactionWriteItems = items
	return cq
}

func (cq *CtxQuery) SetClientRequestToken(token string) *CtxQuery {
	if token != "" {
		cq.clientRequestToken = &token
	}
	return cq
}

func (cq *CtxQuery) SetTransactionGetItems(items []types.TransactGetItem) *CtxQuery {
	cq.transactionGetItems = items
	return cq
}

func (cq *CtxQuery) SetReturnValues(rv types.ReturnValue) *CtxQuery {
	cq.returnValues = rv
	return cq
}

func (cq *CtxQuery) SetReturnValuesOnConditionCheckFailure(rv types.ReturnValuesOnConditionCheckFailure) *CtxQuery {
	cq.returnValuesOnConditionCheckFailure = rv
	return cq
}

func (cq *CtxQuery) SetKeyCondBuilder(kcb *KeyCondBuilder) *CtxQuery {
	cq.keyCondBuilder = kcb
	return cq
}

func (cq *CtxQuery) AppendBatchWriteItems(table string, items []types.WriteRequest) *CtxQuery {
	if cq.batchWriteItems == nil {
		cq.batchWriteItems = make(map[string][]types.WriteRequest, 1)
		cq.batchWriteItems[table] = make([]types.WriteRequest, 0, len(items))
	}
	cq.batchWriteItems[table] = append(cq.batchWriteItems[table], items...)
	return cq
}

func (cq *CtxQuery) AppendTransactionWriteItems(items []types.TransactWriteItem) *CtxQuery {
	if cq.transactionWriteItems == nil {
		cq.transactionWriteItems = make([]types.TransactWriteItem, 0, len(items))
	}
	cq.transactionWriteItems = append(cq.transactionWriteItems, items...)
	return cq
}

func (cq *CtxQuery) AppendTransactionGetItems(items []types.TransactGetItem) *CtxQuery {
	if cq.transactionGetItems == nil {
		cq.transactionGetItems = make([]types.TransactGetItem, 0, len(items))
	}
	cq.transactionGetItems = append(cq.transactionGetItems, items...)
	return cq
}
