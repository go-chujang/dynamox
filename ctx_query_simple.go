package dynamox

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Count call with [query, scan]
func (cq *CtxQuery) Count() *CtxQuery {
	return cq.SetSelectAttr(types.SelectCount)
}

/////////////////////////////////////////////////////////////////////////////
// Get

func (cq *CtxQuery) SimpleGet(table string, key map[string]types.AttributeValue, consistent bool) *CtxQuery {
	return cq.SetTable(table).SetKey(key).SetConsistentRead(consistent)
}

func (cq *CtxQuery) ExprGet(expr expression.Expression) *CtxQuery {
	return cq.SetExprAttrNames(expr.Names())
}

/////////////////////////////////////////////////////////////////////////////
// Query - no Simple suffix method

func (cq *CtxQuery) ExprQuery(expr expression.Expression) *CtxQuery {
	return cq.
		SetExprAttrNames(expr.Names()).
		SetExprAttrValues(expr.Values()).
		SetFilterExpr(expr.Filter()).
		SetKeyCondExpr(expr.KeyCondition()).
		SetProjectExpr(expr.Projection())
}

/////////////////////////////////////////////////////////////////////////////
// Scan - no Simple suffix method

func (cq *CtxQuery) ExprScan(expr expression.Expression) *CtxQuery {
	return cq.
		SetExprAttrNames(expr.Names()).
		SetExprAttrValues(expr.Values()).
		SetFilterExpr(expr.Filter()).
		SetProjectExpr(expr.Projection())
}

/////////////////////////////////////////////////////////////////////////////
// Put

func (cq *CtxQuery) SimplePut(table string, item map[string]types.AttributeValue) *CtxQuery {
	return cq.SetTable(table).SetItem(item)
}

func (cq *CtxQuery) ExprPut(expr expression.Expression) *CtxQuery {
	return cq.SetExprAttrNames(expr.Names()).SetExprAttrValues(expr.Values()).SetCondExpr(expr.Condition())
}

/////////////////////////////////////////////////////////////////////////////
// Update

func (cq *CtxQuery) SimpleUpdate(table string, key map[string]types.AttributeValue) *CtxQuery {
	return cq.SetTable(table).SetKey(key)
}

func (cq *CtxQuery) ExprUpdate(expr expression.Expression) *CtxQuery {
	return cq.
		SetCondExpr(expr.Condition()).
		SetExprAttrNames(expr.Names()).
		SetExprAttrValues(expr.Values()).
		SetUpdateExpr(expr.Update())
}

/////////////////////////////////////////////////////////////////////////////
// Delete

func (cq *CtxQuery) SimpleDelete(table string, key map[string]types.AttributeValue) *CtxQuery {
	return cq.SetTable(table).SetKey(key)
}

func (cq *CtxQuery) ExprDelete(expr expression.Expression) *CtxQuery {
	return cq.SetCondExpr(expr.Condition()).SetExprAttrNames(expr.Names()).SetExprAttrValues(expr.Values())
}

/////////////////////////////////////////////////////////////////////////////
// Batch

func (cq *CtxQuery) SimpleBatchPut(table string, items ...any) *CtxQuery {
	if len(items) == 0 {
		return cq
	}
	puts := make([]types.WriteRequest, 0, len(items))
	for _, v := range items {
		item, err := MarshalMapByAny(v)
		if err != nil {
			return cq.setInsufficientCause(err)
		}
		puts = append(puts, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}
	return cq.SetBatchWriteItems(map[string][]types.WriteRequest{table: puts})
}

func (cq *CtxQuery) SimpleBatchDelete(table string, keys []map[string]types.AttributeValue) *CtxQuery {
	if keys == nil {
		return cq
	}
	dels := make([]types.WriteRequest, 0, len(keys))
	for _, v := range keys {
		dels = append(dels, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: v}})
	}
	return cq.SetBatchWriteItems(map[string][]types.WriteRequest{table: dels})
}

func (cq *CtxQuery) SimpleBatchGet(table string, keys []map[string]types.AttributeValue) *CtxQuery {
	if keys == nil {
		return cq
	}
	return cq.SetBatchGetItems(map[string]types.KeysAndAttributes{table: {Keys: keys}})
}
