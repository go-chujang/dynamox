package example

import (
	"errors"
	"fmt"
	"iter"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/go-chujang/dynamox"
	"github.com/go-chujang/dynamox/dynsa"
)

var (
	_ dynamox.KeyBase         = (*base)(nil)
	_ dynamox.KeyedItem       = (*profile)(nil)
	_ dynamox.KeyedItem       = (*bookmark)(nil)
	_ dynamox.KeyedItemBundle = (*bundle)(nil)
)

// NoSQL Workbench / Bookmarks Data Model

type base struct {
	CustomerId string `dynamodbav:"customerId"`
	Sk         string `dynamodbav:"sk"`
}

func (base) Table() string   { return "CustomerBookmark" }
func (base) PKField() string { return "customerId" }
func (base) SKField() string { return "sk" }
func (b base) PK() any       { return b.CustomerId }
func (b base) SK() any       { return b.Sk }

const profileSortKeyPrefix dynamox.SortKeyPrefix = "CUST"

type profile struct {
	base
	Email           string `dynamodbav:"email"`
	Fullname        string `dynamodbav:"fullName"`
	UserPreferences string `dynamodbav:"userPreferences"`
}

func (p *profile) GetKeyBase() dynamox.KeyBase { return &p.base }
func (p *profile) SaveSK() error {
	if len(p.Sk) == 0 {
		if len(p.CustomerId) == 0 {
			return errors.New("required customerId")
		}
		p.Sk = profileSortKeyPrefix.Composite(p.CustomerId)
	}
	return nil
}

var simpleURLRegexp = regexp.MustCompile(`^https?://[A-Za-z0-9\-\.]+\.[A-Za-z]{2,}(/.*)?$`)

type bookmark struct {
	base
	CreateDate  string `dynamodbav:"createDate"`
	UpdateDate  string `dynamodbav:"updateDate"`
	Folder      string `dynamodbav:"folder"`
	Title       string `dynamodbav:"title"`
	Description string `dynamodbav:"description"`
	Url         string `dynamodbav:"url"`
}

func (b *bookmark) GetKeyBase() dynamox.KeyBase { return &b.base }
func (b *bookmark) SaveSK() error {
	if len(b.Sk) == 0 {
		if !simpleURLRegexp.MatchString(b.Url) {
			return errors.New("bookmark SK must be email-format")
		}
		b.Sk = b.Url
	}
	return nil
}

type bundle struct {
	base
	Profile   profile
	Bookmarks []bookmark

	count int
	items []dynamox.KeyedItem
}

func (b bundle) Count() int { return b.count }
func (b *bundle) Iterator() iter.Seq[dynamox.KeyedItem] {
	if len(b.items) == 0 && b.count > 0 {
		b.items = make([]dynamox.KeyedItem, 0, b.count)
		b.items = append(b.items, &b.Profile)
		for i := range b.Bookmarks {
			b.items = append(b.items, &b.Bookmarks[i])
		}
	}
	return slices.Values(b.items)
}

func (b *bundle) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	if err := dynamox.CheckListOfMaps(av); err != nil {
		return err
	}
	for _, v := range av.(*types.AttributeValueMemberL).Value {
		m := v.(*types.AttributeValueMemberM).Value

		pk, exist := m[b.base.PKField()]
		if !exist {
			return dynamox.ErrUnexpectedPartitionKey
		}
		sk, exist := m[b.base.SKField()]
		if !exist {
			return dynamox.ErrUnexpectedPartitionKey
		}

		var err error
		switch s := sk.(*types.AttributeValueMemberS).Value; {
		case strings.HasPrefix(s, profileSortKeyPrefix.String()):
			err = dynamox.UnmarshalMap(m, &b.Profile)
		case strings.HasPrefix(s, "http"):
			var item bookmark
			err = dynamox.UnmarshalMap(m, &item)
			b.Bookmarks = append(b.Bookmarks, item)
		default:
			err = dynamox.ErrUnexpectedPartitionItem
		}
		if err != nil {
			partitionKey := pk.(*types.AttributeValueMemberS)
			return fmt.Errorf("%T error: %s; partition: %s", *b, err.Error(), partitionKey.Value)
		}
		b.count++
		if b.PK() == "" {
			b.base = base{CustomerId: pk.(*types.AttributeValueMemberS).Value}
		}
	}
	return nil
}

func Test_keyeditem(t *testing.T) {
	var (
		pk123 = base{CustomerId: "123"}
		pk321 = base{CustomerId: "321"}
		table = pk123.Table()
		items = []dynamox.KeyedItem{
			// pk: 123
			&profile{
				base:            pk123,
				Email:           "shirley@example.net",
				Fullname:        "Shirley Rodriguez",
				UserPreferences: "{\"language\": \"en\", \"sort\": \"date\", \"sortDirection\": \"ascending\"}",
			},
			&bookmark{
				base:        pk123,
				CreateDate:  time.Now().Format(time.RFC3339),
				UpdateDate:  time.Now().Format(time.RFC3339),
				Folder:      "Cloud",
				Title:       "AWS",
				Description: "Amazon Web Services",
				Url:         "https://aws.amazon.com",
			},
			&bookmark{
				base:        pk123,
				CreateDate:  time.Now().Format(time.RFC3339),
				UpdateDate:  time.Now().Format(time.RFC3339),
				Folder:      "Cloud",
				Title:       "AWS Console",
				Description: "Web console",
				Url:         "https://console.aws.amazon.com",
			},
			// pk: 321
			&profile{
				base:            pk321,
				Email:           "zhang@example.net",
				Fullname:        "Zhang Wei",
				UserPreferences: "{\"language\": \"zh\", \"sort\": \"rating\", \"sortDirection\": \"descending\"}",
			},
			&bookmark{
				base:        pk321,
				CreateDate:  time.Now().Format(time.RFC3339),
				UpdateDate:  time.Now().Format(time.RFC3339),
				Folder:      "Tools",
				Title:       "AWS",
				Description: "Amazon Web Services",
				Url:         "https://aws.amazon.com",
			},
			&bookmark{
				base:        pk321,
				CreateDate:  time.Now().Format(time.RFC3339),
				UpdateDate:  time.Now().Format(time.RFC3339),
				Folder:      "Docs",
				Title:       "AWS Docs",
				Description: "Documentation",
				Url:         "https://docs.aws.amazon.com",
			},
		}
		gsi_byEmail          = dynamox.MustGSI(dynsa.AttrDefS("email").Aws())
		gsi_byUrl            = dynamox.MustGSI(dynsa.AttrDefS("url").Aws(), dynsa.AttrDefS("customerId").Aws())
		gsi_byCustomerFolder = dynamox.MustGSI(dynsa.AttrDefS("customerId").Aws(), dynsa.AttrDefS("folder").Aws())
	)

	exist, err := cli.TableExists(t.Context(), table)
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		keybase := base{}

		_, err = cli.SDK().CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName:                 aws.String(table),
			DeletionProtectionEnabled: aws.Bool(false),
			BillingMode:               types.BillingModePayPerRequest,
			KeySchema: []types.KeySchemaElement{
				{KeyType: types.KeyTypeHash, AttributeName: aws.String(keybase.PKField())},
				{KeyType: types.KeyTypeRange, AttributeName: aws.String(keybase.SKField())},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{IndexName: aws.String(gsi_byEmail.Name()), KeySchema: gsi_byEmail.KeySchema(), Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll}},
				{IndexName: aws.String(gsi_byUrl.Name()), KeySchema: gsi_byUrl.KeySchema(), Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll}},
				{IndexName: aws.String(gsi_byCustomerFolder.Name()), KeySchema: gsi_byCustomerFolder.KeySchema(), Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll}},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String(keybase.PKField()), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(keybase.SKField()), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(gsi_byEmail.PKField()), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(gsi_byUrl.PKField()), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String(gsi_byCustomerFolder.SKField()), AttributeType: types.ScalarAttributeTypeS},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range items {
		// m, err := dynamox.MarshalMap(v)
		// if err != nil {
		// 	t.Fatal(err)
		// }
		// if err = cli.Put(dynamox.NewCtxQuery(t.Context()).SimplePut(table, m)); err != nil {
		// 	t.Fatal(err)
		// }
		err := cli.Cruder().Create(t.Context(), v, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	profileKeyForRead := profile{base: pk123}
	err = cli.Cruder().Read(t.Context(), &profileKeyForRead)
	if err != nil {
		t.Fatal(err)
	}
	if len(profileKeyForRead.Email) == 0 {
		t.Fatal("failed to read - profile")
	}
	bookmarkKeyForRead := bookmark{base: pk123, Url: "https://aws.amazon.com"}
	err = cli.Cruder().Read(t.Context(), &bookmarkKeyForRead)
	if err != nil {
		t.Fatal(err)
	}
	if len(bookmarkKeyForRead.Title) == 0 {
		t.Fatal("failed to read - bookmark")
	}

	// pk123
	var (
		bundle123 bundle
		query123  = dynamox.NewCtxQuery(t.Context()).SetTable(table).SetKeyCondBuilder(
			dynamox.NewKeyCondBuilder().WithPK(pk123.PKField(), pk123.PK()))
	)
	if _, _, err = cli.Query(query123, &bundle123); err != nil {
		t.Fatal(err)
	}
	// pk321
	var (
		bundle321 bundle
		query321  = dynamox.NewCtxQuery(t.Context()).SetTable(table).SetKeyCondBuilder(
			dynamox.NewKeyCondBuilder().WithPK(pk321.PKField(), pk321.PK()))
	)
	if _, _, err = cli.Query(query321, &bundle321); err != nil {
		panic(err)
	}
	// ByUrl
	var (
		bookmarks      []bookmark
		queryBookmarks = dynamox.NewCtxQuery(t.Context()).
				SetTable(table).
				SetIndex(gsi_byUrl.Name()).
				SetKeyCondBuilder(dynamox.NewKeyCondBuilder().WithPK("url", "https://aws.amazon.com"))
	)
	if _, _, err = cli.Query(queryBookmarks, &bookmarks); err != nil {
		panic(err)
	}
}
