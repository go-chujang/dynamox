package example

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/go-chujang/dynamox"
)

var cli *dynamox.Client

func init() {
	var (
		accesskey = "test"
		secretkey = "test"
		region    = "ap-northeast-2"
		localAddr = "http://localhost:8000"
		cred      = credentials.NewStaticCredentialsProvider(accesskey, secretkey, "")
		awsCfg, _ = config.LoadDefaultConfig(
			context.Background(),
			config.WithRegion(region),
			config.WithCredentialsProvider(cred),
			config.WithBaseEndpoint(localAddr),
		)
	)
	cli = dynamox.NewClient(awsCfg)
}

func Test_cleanup(t *testing.T) {
	ctx := context.Background()
	list, err := cli.TableList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range list {
		_, err = cli.SDK().DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(v)})
		if err != nil {
			t.Fatal(err)
		}
	}
}
