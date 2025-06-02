package dynamox

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (cq CtxQuery) get() (*dynamodb.GetItemInput, error) {
	if !cq.required(cq.key).isValidWithTable() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.GetItemInput{
		Key:                      cq.key,
		TableName:                aws.String(cq.tableName),
		ConsistentRead:           aws.Bool(cq.consistentRead),
		ExpressionAttributeNames: cq.exprAttrNames,
		ProjectionExpression:     cq.projectExpr,
	}, nil
}

func (cq CtxQuery) query() (*dynamodb.QueryInput, error) {
	if !cq.required(cq.selectAttr).isValidWithTable() {
		return nil, cq.errWithInsufficient()
	}
	if cq.keyCondBuilder != nil {
		expr, err := cq.keyCondBuilder.Build()
		if err != nil {
			return nil, err
		}
		cq.ExprQuery(expr)
	}
	return &dynamodb.QueryInput{
		TableName:                 aws.String(cq.tableName),
		ConsistentRead:            aws.Bool(cq.consistentRead),
		ExclusiveStartKey:         cq.startKey,
		ExpressionAttributeNames:  cq.exprAttrNames,
		ExpressionAttributeValues: cq.exprAttrValues,
		FilterExpression:          cq.filterExpr,
		IndexName:                 cq.index,
		KeyConditionExpression:    cq.keyCondExpr,
		Limit:                     cq.limit,
		ProjectionExpression:      cq.projectExpr,
		ScanIndexForward:          aws.Bool(cq.orderByAsc),
		Select:                    cq.selectAttr,
	}, nil
}

func (cq CtxQuery) scan() (*dynamodb.ScanInput, error) {
	if !cq.required(cq.selectAttr).isValidWithTable() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.ScanInput{
		TableName:                 aws.String(cq.tableName),
		ConsistentRead:            aws.Bool(cq.consistentRead),
		ExclusiveStartKey:         cq.startKey,
		ExpressionAttributeNames:  cq.exprAttrNames,
		ExpressionAttributeValues: cq.exprAttrValues,
		FilterExpression:          cq.filterExpr,
		IndexName:                 cq.index,
		Limit:                     cq.limit,
		ProjectionExpression:      cq.projectExpr,
		Select:                    cq.selectAttr,
	}, nil
}

func (cq CtxQuery) put() (*dynamodb.PutItemInput, error) {
	if !cq.required(cq.item).isValidWithTable() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.PutItemInput{
		Item:                                cq.item,
		TableName:                           aws.String(cq.tableName),
		ConditionExpression:                 cq.condExpr,
		ExpressionAttributeNames:            cq.exprAttrNames,
		ExpressionAttributeValues:           cq.exprAttrValues,
		ReturnValues:                        cq.returnValues,
		ReturnValuesOnConditionCheckFailure: cq.returnValuesOnConditionCheckFailure,
	}, nil
}

func (cq CtxQuery) update() (*dynamodb.UpdateItemInput, error) {
	if !cq.required(cq.key).isValidWithTable() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.UpdateItemInput{
		Key:                                 cq.key,
		TableName:                           aws.String(cq.tableName),
		ConditionExpression:                 cq.condExpr,
		ExpressionAttributeNames:            cq.exprAttrNames,
		ExpressionAttributeValues:           cq.exprAttrValues,
		ReturnValues:                        cq.returnValues,
		ReturnValuesOnConditionCheckFailure: cq.returnValuesOnConditionCheckFailure,
		UpdateExpression:                    cq.updateExpr,
	}, nil
}

func (cq CtxQuery) delete() (*dynamodb.DeleteItemInput, error) {
	if !cq.required(cq.key).isValidWithTable() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.DeleteItemInput{
		Key:                                 cq.key,
		TableName:                           aws.String(cq.tableName),
		ConditionExpression:                 cq.condExpr,
		ExpressionAttributeNames:            cq.exprAttrNames,
		ExpressionAttributeValues:           cq.exprAttrValues,
		ReturnValues:                        cq.returnValues,
		ReturnValuesOnConditionCheckFailure: cq.returnValuesOnConditionCheckFailure,
	}, nil
}

func (cq CtxQuery) batchWrite() (*dynamodb.BatchWriteItemInput, error) {
	if !cq.required(cq.batchWriteItems).isValid() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.BatchWriteItemInput{RequestItems: cq.batchWriteItems}, nil
}

func (cq CtxQuery) batchGet() (*dynamodb.BatchGetItemInput, error) {
	if !cq.required(cq.batchGetItems).isValid() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.BatchGetItemInput{RequestItems: cq.batchGetItems}, nil
}

func (cq CtxQuery) transactionWrite() (*dynamodb.TransactWriteItemsInput, error) {
	if !cq.required(cq.transactionWriteItems).isValid() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.TransactWriteItemsInput{
		TransactItems:      cq.transactionWriteItems,
		ClientRequestToken: cq.clientRequestToken,
	}, nil
}

func (cq CtxQuery) transactionGet() (*dynamodb.TransactGetItemsInput, error) {
	if !cq.required(cq.transactionGetItems).isValid() {
		return nil, cq.errWithInsufficient()
	}
	return &dynamodb.TransactGetItemsInput{TransactItems: cq.transactionGetItems}, nil
}
