package dynamox

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c *Client) TableExists(ctx context.Context, name string) (bool, error) {
	exists := true
	_, err := c.SDK().DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		var ae *types.TableAlreadyExistsException
		if errors.As(err, &ae) {
			return exists, nil
		}

		var nf *types.TableNotFoundException
		var rnf *types.ResourceNotFoundException
		if errors.As(err, &nf) || errors.As(err, &rnf) {
			exists = false
			err = nil
		}
	}
	return exists, err
}

func (c *Client) TableList(ctx context.Context, limitOps ...int32) (list []string, err error) {
	limit := int32(100)
	if limitOps != nil && limitOps[0] > 0 {
		limit = limitOps[0]
	}
	out, err := c.SDK().ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(limit)})
	if err != nil {
		return nil, err
	}
	_ = out.LastEvaluatedTableName
	return out.TableNames, nil
}

func (c *Client) TableApproximateItemCount(ctx context.Context, name string) (int64, error) {
	out, err := c.SDK().DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		return 0, err
	}
	return *out.Table.ItemCount, nil
}
