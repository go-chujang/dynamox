package dynsa

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type (
	scalarT types.ScalarAttributeType
	attrDef types.AttributeDefinition
)

func (alias scalarT) Aws() types.ScalarAttributeType { return types.ScalarAttributeType(alias) }
func (alias attrDef) Aws() types.AttributeDefinition { return types.AttributeDefinition(alias) }

func AttrDef(name string, styp scalarT) attrDef {
	return attrDef{AttributeName: toPtrOrNil(name), AttributeType: styp.Aws()}
}

func AttrDefS(name string) attrDef {
	return attrDef{AttributeName: toPtrOrNil(name), AttributeType: ScalarS.Aws()}
}

func AttrDefN(name string) attrDef {
	return attrDef{AttributeName: toPtrOrNil(name), AttributeType: ScalarN.Aws()}
}

func AttrDefB(name string) attrDef {
	return attrDef{AttributeName: toPtrOrNil(name), AttributeType: ScalarB.Aws()}
}
