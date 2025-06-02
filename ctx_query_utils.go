package dynamox

import (
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (cq CtxQuery) isValid() bool          { return !cq.insufficient }
func (cq CtxQuery) isValidWithTable() bool { return cq.isValid() && cq.tableName != "" }

func (cq *CtxQuery) setInsufficient(cond ...bool) *CtxQuery {
	if cond == nil {
		cq.insufficient = true
	} else {
		cq.insufficient = cond[0]
	}
	return cq
}

func (cq *CtxQuery) setInsufficientCause(cause error) *CtxQuery {
	cq.insufficientCause = cause
	return cq.setInsufficient()
}

func (i *CtxQuery) errWithInsufficient(err ...error) error {
	if i.insufficientCause == nil && err == nil {
		return ErrInsufficientRequiredMembers
	}
	n := 1
	if i.insufficientCause != nil {
		n++
	}
	if len(err) > 0 {
		n += len(err)
	}

	errs := make([]error, 0, n)
	errs = append(errs, ErrInsufficientRequiredMembers)
	if i.insufficientCause != nil {
		errs = append(errs, i.insufficientCause)
	}
	if len(err) > 0 {
		errs = append(errs, err...)
	}
	return errors.Join(errs...)
}

// q.tableName is checked by validWithTableName()
func (cq *CtxQuery) required(member any) *CtxQuery {
	if cq.insufficient {
		return cq
	}
	// todo: setInsufficient() -> setInsufficientCause()
	switch v := member.(type) {
	case map[string][]types.WriteRequest:
		if v == nil {
			return cq.setInsufficient()
		}
		inc := 0
		for _, wrqs := range v { // todo: check PutRequest:Item or DeleteRequest:Key
			if inc > BatchWriteLimit {
				return cq.setInsufficient()
			}
			inc += len(wrqs)
		}
		if inc == 0 {
			return cq.setInsufficient()
		}
	case map[string]types.KeysAndAttributes:
		if v == nil {
			return cq.setInsufficient()
		}
		inc := 0
		for _, attrs := range v {
			if inc > BatchGetLimit {
				return cq.setInsufficient()
			}
			if attrs.Keys == nil {
				return cq.setInsufficient()
			}
			inc += len(attrs.Keys)
		}
		if inc == 0 {
			return cq.setInsufficient()
		}
	case types.Select:
		if v == types.SelectSpecificAttributes {
			return cq.setInsufficient(cq.projectExpr == nil)
		}
	case []types.TransactWriteItem:
		if v == nil || len(v) > TransactionWriteLimit {
			return cq.setInsufficient()
		}
	case []types.TransactGetItem:
		if v == nil || len(v) > TransactionGetLimit {
			return cq.setInsufficient()
		}
	default:
		return cq.setInsufficient(reflect.ValueOf(member).IsZero())
	}
	return cq
}
