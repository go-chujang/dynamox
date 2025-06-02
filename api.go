package dynamox

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c *Client) Get(query *CtxQuery, output any) error {
	if !isNonNilPointer(output) {
		return ErrOutputNilPointer
	}

	parsed, err := query.get()
	if err != nil {
		return err
	}
	switch out, err := c.SDK().GetItem(query.Context(), parsed); {
	case err != nil:
		return err
	case len(out.Item) == 0:
		return ErrNotFoundItem
	default:
		return UnmarshalMapByAny(out.Item, output)
	}
}

func (c *Client) Query(query *CtxQuery, output any) (int32, PaginationKey, error) {
	if query.selectAttr != types.SelectCount && !isNonNilPointer(output) {
		return 0, nil, ErrOutputNilPointer
	}

	parsed, err := query.query()
	if err != nil {
		return 0, nil, err
	}
	out, err := c.SDK().Query(query.Context(), parsed)
	if err != nil {
		return 0, nil, err
	}
	if query.selectAttr != types.SelectCount && out.Count > 0 {
		err = UnmarshalListOfMapsByAny(out.Items, output)
	}
	return out.Count, out.LastEvaluatedKey, err
}

func (c *Client) Scan(query *CtxQuery, output any) (int32, PaginationKey, error) {
	if query.selectAttr != types.SelectCount && !isNonNilPointer(output) {
		return 0, nil, ErrOutputNilPointer
	}

	parsed, err := query.scan()
	if err != nil {
		return 0, nil, err
	}
	out, err := c.SDK().Scan(query.Context(), parsed)
	if err != nil {
		return 0, nil, err
	}
	if query.selectAttr != types.SelectCount && out.Count > 0 {
		err = UnmarshalListOfMapsByAny(out.Items, output)
	}
	return out.Count, out.LastEvaluatedKey, err
}

func (c *Client) Put(query *CtxQuery, outputOps ...any) error {
	if len(outputOps) > 0 && !isNonNilPointer(outputOps[0]) {
		return ErrOutputNilPointer
	}
	if !isEnableNonUpdateReturnValues(query.returnValues) {
		return ErrReturnValuesSetToInvalidValue
	}

	parsed, err := query.put()
	if err != nil {
		return err
	}
	switch out, err := c.SDK().PutItem(query.Context(), parsed); {
	case err != nil || len(outputOps) == 0:
		return err
	case query.returnValues == "" && query.returnValuesOnConditionCheckFailure == "":
		return ErrReturnValuesNotSet
	default:
		return UnmarshalMapByAny(out.Attributes, outputOps[0])
	}
}

func (c *Client) Update(query *CtxQuery, outputOps ...any) error {
	if len(outputOps) > 0 && !isNonNilPointer(outputOps[0]) {
		return ErrOutputNilPointer
	}

	parsed, err := query.update()
	if err != nil {
		return err
	}
	switch out, err := c.SDK().UpdateItem(query.Context(), parsed); {
	case err != nil || len(outputOps) == 0:
		return err
	case query.returnValues == "" && query.returnValuesOnConditionCheckFailure == "":
		return ErrReturnValuesNotSet
	default:
		return UnmarshalMapByAny(out.Attributes, outputOps[0])
	}
}

func (c *Client) Delete(query *CtxQuery, outputOps ...any) error {
	if len(outputOps) > 0 && !isNonNilPointer(outputOps[0]) {
		return ErrOutputNilPointer
	}
	if !isEnableNonUpdateReturnValues(query.returnValues) {
		return ErrReturnValuesSetToInvalidValue
	}

	parsed, err := query.delete()
	if err != nil {
		return err
	}
	switch out, err := c.SDK().DeleteItem(query.Context(), parsed); {
	case err != nil || len(outputOps) == 0:
		return err
	case query.returnValues == "" && query.returnValuesOnConditionCheckFailure == "":
		return ErrReturnValuesNotSet
	default:
		return UnmarshalMapByAny(out.Attributes, outputOps[0])
	}
}

func (c *Client) BatchWrite(query *CtxQuery) (map[string][]types.WriteRequest, error) {
	var unprocessedItems map[string][]types.WriteRequest
	err := c.BatchWriteWithCallBack(query, func(bwio *dynamodb.BatchWriteItemOutput) error {
		unprocessedItems = bwio.UnprocessedItems
		return nil
	})
	if len(unprocessedItems) > 0 {
		err = errors.Join(err, ErrUnprocessedItems)
	}
	return unprocessedItems, err
}

func (c *Client) BatchWriteWithCallBack(query *CtxQuery, callback BatchWriteCallbackFn) error {
	parsed, err := query.batchWrite()
	if err != nil {
		return err
	}
	out, err := c.SDK().BatchWriteItem(query.Context(), parsed)
	if err != nil {
		return err
	}
	return callback(out)
}

func (c *Client) BatchGet(query *CtxQuery) (map[string][]map[string]types.AttributeValue, error) {
	var responses map[string][]map[string]types.AttributeValue
	err := c.BatchGetWithCallBack(query, func(bgio *dynamodb.BatchGetItemOutput) error {
		responses = bgio.Responses
		return nil
	})
	return responses, err
}

func (c *Client) BatchGetWithCallBack(query *CtxQuery, callback BatchGetCallbackFn) error {
	parsed, err := query.batchGet()
	if err != nil {
		return err
	}
	out, err := c.SDK().BatchGetItem(query.Context(), parsed)
	if err != nil {
		return err
	}
	return callback(out)
}

func (c *Client) TransactionWrite(query *CtxQuery) error {
	parsed, err := query.transactionWrite()
	if err != nil {
		return err
	}
	_, err = c.SDK().TransactWriteItems(query.Context(), parsed)
	return err
}

func (c *Client) TransactionGet(query *CtxQuery) ([]map[string]types.AttributeValue, error) {
	var orderedItems []map[string]types.AttributeValue
	err := c.TransactionGetWithCallBack(query, func(tgio *dynamodb.TransactGetItemsOutput) error {
		orderedItems := make([]map[string]types.AttributeValue, len(tgio.Responses))
		for i, v := range tgio.Responses {
			orderedItems[i] = v.Item
		}
		return nil
	})
	return orderedItems, err
}

func (c *Client) TransactionGetWithCallBack(query *CtxQuery, callback TransactionGetCallbackFn) error {
	parsed, err := query.transactionGet()
	if err != nil {
		return err
	}
	out, err := c.SDK().TransactGetItems(query.Context(), parsed)
	if err != nil {
		return err
	}
	return callback(out)
}
