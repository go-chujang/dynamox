package example

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/go-chujang/dynamox"
	"github.com/google/uuid"
)

func Test_apitest(t *testing.T) {
	type FooBar struct {
		Foo string `dynamodbav:"foo"`
		Bar int64  `dynamodbav:"bar"`
	}
	var (
		table        = "apitesttable"
		newTestData  = func() FooBar { return FooBar{Foo: uuid.NewString(), Bar: time.Now().UnixNano()} }
		convert2Attr = func(v FooBar) map[string]types.AttributeValue {
			m, err := dynamox.MarshalMapByAny(v)
			if err != nil {
				t.Fatal(err)
			}
			return m
		}
	)
	exist, err := cli.TableExists(t.Context(), table)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		pk := types.AttributeDefinition{AttributeName: aws.String("foo"), AttributeType: types.ScalarAttributeTypeS}
		sk := types.AttributeDefinition{AttributeName: aws.String("bar"), AttributeType: types.ScalarAttributeTypeN}
		_, err = cli.SDK().CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName:                 aws.String(table),
			DeletionProtectionEnabled: aws.Bool(false),
			KeySchema:                 []types.KeySchemaElement{{KeyType: types.KeyTypeHash, AttributeName: pk.AttributeName}, {KeyType: types.KeyTypeRange, AttributeName: sk.AttributeName}},
			AttributeDefinitions:      []types.AttributeDefinition{{AttributeName: pk.AttributeName, AttributeType: pk.AttributeType}, {AttributeName: sk.AttributeName, AttributeType: sk.AttributeType}},
			LocalSecondaryIndexes:     nil,
			GlobalSecondaryIndexes:    nil,
			BillingMode:               types.BillingModePayPerRequest,
			ProvisionedThroughput:     nil,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var (
		equalFn  = func(x, y FooBar) bool { return x.Foo == y.Foo && x.Bar == y.Bar }
		testData = []FooBar{
			newTestData(),
			newTestData(),
			newTestData(),
		}
	)

	// Put
	for _, v := range testData {
		query := dynamox.NewCtxQuery(t.Context()).SimplePut(table, convert2Attr(v))
		err = cli.Put(query)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Get
	for _, v := range testData {
		var errorCase FooBar
		if nil == cli.Get(
			dynamox.NewCtxQuery().SetTable(table).SetKey(map[string]types.AttributeValue{
				"foo": &types.AttributeValueMemberS{Value: v.Foo},
			}),
			&errorCase,
		) { // error: ValidationException: The number of conditions on the keys is invalid
			t.Fail()
		}

		var foo FooBar
		query := dynamox.NewCtxQuery(t.Context()).SimpleGet(table, convert2Attr(v), false)
		if err = cli.Get(query, &foo); err != nil {
			t.Fatal(err)
		}
		if !equalFn(v, foo) {
			t.Fail()
		}
	}
	// Query
	for _, v := range testData {
		expr, err := expression.NewBuilder().
			WithKeyCondition(
				expression.Key("foo").Equal(expression.Value(v.Foo)).And(
					expression.Key("bar").Equal(expression.Value(v.Bar)),
				),
			).Build()
		if err != nil {
			t.Fatal(err)
		}
		var foo []FooBar
		_, _, err = cli.Query(dynamox.NewCtxQuery(t.Context()).SetTable(table).ExprQuery(expr), &foo)
		if err != nil {
			t.Fatal(err)
		}
		for _, vv := range foo {
			if !equalFn(v, vv) {
				t.Fail()
			}
		}
	}
	// Scan & Count
	var (
		scanOutput  []FooBar
		scanCount   int32
		selectCount int32
		apprCount   int64
	)
	scanCount, _, err = cli.Scan(dynamox.NewCtxQuery(t.Context()).SetTable(table), &scanOutput)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range testData {
		ok := false
		for _, vv := range scanOutput {
			if equalFn(v, vv) {
				ok = true
				break
			}
		}
		if !ok {
			t.Fail()
		}
	}
	selectCount, _, err = cli.Scan(dynamox.NewCtxQuery(t.Context()).SetTable(table).Count().SetConsistentRead(true), nil)
	if err != nil {
		t.Fatal(err)
	}
	apprCount, err = cli.TableApproximateItemCount(t.Context(), table)
	if err != nil {
		t.Fatal(err)
	}
	if scanCount != selectCount && scanCount != int32(apprCount) {
		t.Fail()
	}
	// Update
	type updateFooBar struct {
		FooBar
		Idx int `dynamodbav:"idx"`
	}
	for idx, v := range testData {
		uv := updateFooBar{
			FooBar: v,
			Idx:    idx,
		}
		expr, err := expression.NewBuilder().WithUpdate(
			expression.Set(expression.Name("idx"), expression.Value(uv.Idx)),
		).Build()
		if err != nil {
			t.Fatal(err)
		}
		query := dynamox.NewCtxQuery(t.Context()).
			SimpleUpdate(table, convert2Attr(v)).
			ExprUpdate(expr).
			SetReturnValues(types.ReturnValueAllNew)
		var allnew updateFooBar
		err = cli.Update(query, &allnew)
		if err != nil {
			t.Fatal(err)
		}
		if idx != allnew.Idx || !equalFn(uv.FooBar, allnew.FooBar) {
			t.Fail()
		}
	}
	// Delete
	for _, v := range testData {
		query := dynamox.NewCtxQuery(t.Context()).SimpleDelete(table, convert2Attr(v)).SetReturnValues(types.ReturnValueAllOld)
		var allold FooBar
		err = cli.Delete(query, &allold)
		if err != nil {
			t.Fatal(err)
		}
		if !equalFn(v, allold) {
			t.Fail()
		}
	}
	// Batch Put
	batchWriteList := make([]any, 0, dynamox.BatchWriteLimit)
	for range dynamox.BatchWriteLimit {
		batchWriteList = append(batchWriteList, newTestData())
	}
	batchPutQuery := dynamox.NewCtxQuery(t.Context()).SimpleBatchPut(table, batchWriteList...)
	err = cli.BatchWriteWithCallBack(batchPutQuery, func(bwio *dynamodb.BatchWriteItemOutput) error {
		if len(bwio.UnprocessedItems) > 0 {
			return errors.New("unexpected case")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Batch Get
	batchGetKeys := make([]map[string]types.AttributeValue, 0, len(batchWriteList))
	for _, v := range batchWriteList {
		batchGetKeys = append(batchGetKeys, convert2Attr(v.(FooBar)))
	}
	batchGetQuery := dynamox.NewCtxQuery(t.Context()).SimpleBatchGet(table, batchGetKeys)
	err = cli.BatchGetWithCallBack(batchGetQuery, func(bgio *dynamodb.BatchGetItemOutput) error {
		for _, v := range bgio.Responses[table] {
			var out FooBar
			if err := dynamox.UnmarshalMapByAny(v, &out); err != nil {
				return err
			}
			ok := false
			for _, v := range batchWriteList {
				if equalFn(v.(FooBar), out) {
					ok = true
					break
				}
			}
			if !ok {
				return errors.New("unexpected case")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// All Delete
	var (
		allDeleteScanQuery = dynamox.NewCtxQuery(t.Context()).SetTable(table).SetConsistentRead(true)
		allDeleteList      []FooBar
		allDeleteCount     int32
	)
	allDeleteCount, _, err = cli.Scan(allDeleteScanQuery, &allDeleteList)
	if err != nil {
		t.Fatal(err)
	}
	if len(allDeleteList) != int(allDeleteCount) {
		t.Fail()
	}

	start := 0
	end := start + dynamox.BatchWriteLimit
	for start < int(allDeleteCount) {
		if end > int(allDeleteCount) {
			end = int(allDeleteCount)
		}

		keys := make([]map[string]types.AttributeValue, 0, end-start)
		for _, v := range allDeleteList[start:end] {
			keys = append(keys, convert2Attr(v))
		}
		query := dynamox.NewCtxQuery(t.Context()).SimpleBatchDelete(table, keys)

		err = cli.BatchWriteWithCallBack(query, func(bwio *dynamodb.BatchWriteItemOutput) error {
			if bwio.UnprocessedItems == nil {
				return errors.New("unexpected case")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		start = end
		end += dynamox.BatchWriteLimit
	}
	expectZero, _, err := cli.Scan(dynamox.NewCtxQuery(t.Context()).SetTable(table).Count().SetConsistentRead(true), nil)
	if err != nil {
		t.Fatal(err)
	}
	if expectZero != 0 {
		t.Fail()
	}
}
