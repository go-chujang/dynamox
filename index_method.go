package dynamox

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (i Index) Name() string {
	return i.name
}

func (i Index) Kind() indexKind {
	if strings.HasPrefix(i.name, GSI.String()) {
		return GSI
	}
	return LSI
}

func (i Index) PKField() (pkfield string) {
	if i.pkDef.AttributeName != nil {
		pkfield = *i.pkDef.AttributeName
	}
	return pkfield
}

func (i Index) SKField() (skField string) {
	if i.skDef.AttributeName != nil {
		skField = *i.skDef.AttributeName
	}
	return skField
}

func (i Index) SKDef() types.AttributeDefinition {
	return i.skDef
}

func (i Index) KeySchema() []types.KeySchemaElement {
	keySchemes := []types.KeySchemaElement{{
		KeyType:       types.KeyTypeHash,
		AttributeName: i.pkDef.AttributeName,
	}}
	if i.skDef.AttributeName != nil {
		keySchemes = append(keySchemes, types.KeySchemaElement{
			KeyType:       types.KeyTypeRange,
			AttributeName: i.skDef.AttributeName,
		})
	}
	return keySchemes
}

func (i Index) KeyAttrDef() []types.AttributeDefinition {
	attrDefs := []types.AttributeDefinition{i.pkDef}
	if i.skDef.AttributeName != nil {
		attrDefs = append(attrDefs, i.skDef)
	}
	return attrDefs
}
