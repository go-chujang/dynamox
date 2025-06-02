package dynamox

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

type conditionOperator int

const (
	Equal conditionOperator = iota
	LessThan
	LessThanEqual
	GreaterThan
	GreaterThanEqual
	BeginsWith
	Between
)

func (cond conditionOperator) keyCondBuilder(key string, value any, betweenUpper any) (condBuilder expression.KeyConditionBuilder, err error) {
	if key == "" || value == nil {
		return condBuilder, ErrRequiredKeyAndValue
	}

	keyBuilder := expression.Key(key)
	switch cond {
	case LessThan:
		condBuilder = keyBuilder.LessThan(expression.Value(value))
	case LessThanEqual:
		condBuilder = keyBuilder.LessThanEqual(expression.Value(value))
	case GreaterThan:
		condBuilder = keyBuilder.GreaterThan(expression.Value(value))
	case GreaterThanEqual:
		condBuilder = keyBuilder.GreaterThanEqual(expression.Value(value))
	case BeginsWith:
		if prefix, ok := value.(string); ok {
			condBuilder = keyBuilder.BeginsWith(prefix)
		} else {
			err = ErrBeginsWithPrefixType
		}
	case Between:
		if betweenUpper != nil {
			condBuilder = keyBuilder.Between(expression.Value(value), expression.Value(betweenUpper))
		} else {
			err = ErrBetweenUpperValue
		}
	default:
		condBuilder = keyBuilder.Equal(expression.Value(value))
	}
	return
}
