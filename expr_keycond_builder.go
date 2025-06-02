package dynamox

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type KeyCondBuilder struct {
	partitionKeyField   string
	partitionKeyValue   any
	sortKeyField        string
	sortKeyValue        any
	sortKeyBetweenUpper any
	sortKeyCondition    conditionOperator
	projectionFieldList []string
	// partitionKeyCondition conditionOperator // always equal
}

func NewKeyCondBuilder() *KeyCondBuilder {
	return &KeyCondBuilder{}
}

func (kcb *KeyCondBuilder) WithPK(field string, value any) *KeyCondBuilder {
	kcb.partitionKeyField = field
	kcb.partitionKeyValue = value
	return kcb
}

func (kcb *KeyCondBuilder) WithSK(cond conditionOperator, field string, value any, betweenUpper ...any) *KeyCondBuilder {
	kcb.sortKeyCondition = cond
	kcb.sortKeyField = field
	kcb.sortKeyValue = value
	if len(betweenUpper) > 0 {
		kcb.sortKeyBetweenUpper = betweenUpper[0]
	}
	return kcb
}

func (kcb *KeyCondBuilder) WithKeyBase(base KeyBase, skCond conditionOperator, betweenUpper ...any) *KeyCondBuilder {
	return kcb.
		WithPK(base.PKField(), base.PK()).
		WithSK(skCond, base.SKField(), base.SK(), betweenUpper...)
}

func (kcb *KeyCondBuilder) WithProj(projs ...string) *KeyCondBuilder {
	kcb.projectionFieldList = append(kcb.projectionFieldList, projs...)
	return kcb
}

func (kcb KeyCondBuilder) IsEnable() bool {
	return len(kcb.partitionKeyField) > 0 && kcb.partitionKeyValue != nil
}

func (kcb KeyCondBuilder) Build() (expression.Expression, error) {
	if !kcb.IsEnable() {
		return expression.Expression{}, ErrRequiredPartitionKey
	}

	builder := expression.NewBuilder()
	keyCond, err := Equal.keyCondBuilder(kcb.partitionKeyField, kcb.partitionKeyValue, nil)
	if err != nil {
		return expression.Expression{}, err
	}
	if kcb.sortKeyField != "" && kcb.sortKeyValue != nil {
		skCond, err := kcb.sortKeyCondition.keyCondBuilder(kcb.sortKeyField, kcb.sortKeyValue, kcb.sortKeyBetweenUpper)
		if err != nil {
			return expression.Expression{}, err
		}
		keyCond = keyCond.And(skCond)
	}
	if l := len(kcb.projectionFieldList); l > 0 {
		proj := expression.NamesList(expression.Name(kcb.projectionFieldList[0]))
		if l > 1 {
			dedupl := make(map[string]struct{}, l-1)
			for _, v := range kcb.projectionFieldList[1:] {
				if _, exist := dedupl[v]; exist {
					continue
				}
				proj = expression.AddNames(proj, expression.Name(v))
				dedupl[v] = struct{}{}
			}
		}
		builder = builder.WithProjection(proj)
	}
	return builder.WithKeyCondition(keyCond).Build()
}
