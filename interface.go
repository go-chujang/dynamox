package dynamox

import (
	"iter"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

type KeyBase interface {
	Table() string   // return Table Name
	PKField() string // return PartitionKey Field Name
	SKField() string // return SortKey Field Name
	PK() any         // return PartitionKey value
	SK() any         // return SortKey value
}

type KeyBaseHooker interface { // for initialize PartitionKey
	PreMarshal() error
	PostUnmarshal() error
}

type KeyedItem interface {
	KeyBase
	GetKeyBase() KeyBase
	// call SaveSK() on each new KeyedItem to initialize its SortKey
	SaveSK() error
}

// read-only
//
// use with attributevalue.UnmarshalListOfMaps
type KeyedItemBundle interface {
	KeyBase
	attributevalue.Unmarshaler
	Count() int
	Iterator() iter.Seq[KeyedItem]
}
