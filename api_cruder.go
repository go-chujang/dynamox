package dynamox

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

func (c *cruder) Exist(ctx context.Context, keyedItem KeyedItem, skipSK bool, consistent ...bool) (bool, error) {
	err := Prepare(keyedItem, skipSK)
	if err != nil {
		return false, err
	}

	keyCond := expression.Key(keyedItem.PKField()).Equal(expression.Value(keyedItem.PK()))
	if !skipSK {
		keyCond = keyCond.And(expression.Key(keyedItem.SKField()).Equal(expression.Value(keyedItem.SK())))
	}
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		return false, err
	}
	query := NewCtxQuery(ctx).
		SetTable(keyedItem.Table()).
		ExprQuery(expr).
		SetLimit(1).
		Count()
	if len(consistent) > 0 {
		query.SetConsistentRead(consistent[0])
	}
	cnt, _, err := c.cli().Query(query, nil)
	return cnt == 1, err
}

func (c *cruder) Create(ctx context.Context, keyedItem KeyedItem, strictPk bool) error {
	item, err := MarshalMap(keyedItem)
	if err != nil {
		return err
	}

	query := NewCtxQuery(ctx).SimplePut(keyedItem.Table(), item)
	if strictPk {
		cond := fmt.Sprintf("attribute_not_exists(%s)", keyedItem.PKField())
		query.SetCondExpr(&cond)
	}
	return c.cli().Put(query)
}

func (c *cruder) Read(ctx context.Context, keyedItem KeyedItem, consistent ...bool) error {
	key, err := MarshalMapOnlyKey(keyedItem)
	if err != nil {
		return err
	}
	query := NewCtxQuery(ctx).SetTable(keyedItem.Table()).SetKey(key)
	if len(consistent) > 0 {
		query.SetConsistentRead(consistent[0])
	}
	return c.cli().Get(query, keyedItem)
}

func (c *cruder) Update(ctx context.Context, keyedItem KeyedItem, strictPk bool) error {
	key, err := MarshalMapOnlyKey(keyedItem)
	if err != nil {
		return err
	}
	SetUpdatedAt(keyedItem)

	var cond []expression.ConditionBuilder
	if strictPk {
		cond = append(cond, expression.Name(keyedItem.PKField()).AttributeExists())
	}
	expr, err := KeyedItem2UpdateExpr(keyedItem, cond...)
	if err != nil {
		return err
	}
	query := NewCtxQuery(ctx).SetTable(keyedItem.Table()).SetKey(key).ExprUpdate(expr)
	return c.cli().Update(query)
}

func (c *cruder) Delete(ctx context.Context, keyedItem KeyedItem) error {
	key, err := MarshalMapOnlyKey(keyedItem)
	if err != nil {
		return err
	}
	query := NewCtxQuery(ctx).SimpleDelete(keyedItem.Table(), key)
	return c.cli().Delete(query)
}

func (c *cruder) DeleteSoft(ctx context.Context, keyedItem KeyedItem) error {
	if !HasModel(keyedItem) {
		return ErrUnembedModel
	}
	deletedAt := TimeNow().Int64()

	key, err := MarshalMapOnlyKey(keyedItem)
	if err != nil {
		return err
	}
	expr, err := expression.NewBuilder().WithUpdate(expression.
		Set(expression.Name(keyedItem.PKField()), expression.Value(keyedItem.PK())).
		Set(expression.Name(keyedItem.SKField()), expression.Value(keyedItem.SK())).
		Set(expression.Name("deletedAt"), expression.Value(deletedAt)),
	).Build()
	if err != nil {
		return err
	}
	query := NewCtxQuery(ctx).SetTable(keyedItem.Table()).SetKey(key).ExprUpdate(expr)
	return c.cli().Update(query)
}
