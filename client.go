package dynamox

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Client struct {
	client *dynamodb.Client
}

func (c *Client) SDK() *dynamodb.Client { return c.client }

func NewClient(awsCfg aws.Config, optFns ...func(*dynamodb.Options)) *Client {
	cli := dynamodb.NewFromConfig(awsCfg, optFns...)
	return &Client{
		client: cli,
	}
}

type cruder Client

func (c *Client) Cruder() *cruder { return (*cruder)(c) }
func (c *cruder) cli() *Client    { return (*Client)(c) }
