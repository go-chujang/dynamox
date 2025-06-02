package dynamox

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// todo: proj & attrdef

type indexKind int

const (
	GSI indexKind = iota
	LSI
)

func (k indexKind) String() string {
	if k == GSI {
		return "gsi"
	}
	return "lsi"
}

const indexNameSep = "-"

type Index struct {
	// {kind}-{pkField}-{skField(if-exist)}
	name  string
	pkDef types.AttributeDefinition
	skDef types.AttributeDefinition
}

func NewIndex(kind indexKind, pkDef types.AttributeDefinition, skDefOps ...types.AttributeDefinition) (Index, error) {
	var skDef types.AttributeDefinition
	if len(skDefOps) > 0 {
		skDef = skDefOps[0]
	}
	switch {
	case pkDef.AttributeName == nil:
		return Index{}, ErrInvalidAttributeDefinition
	case kind == GSI:
	case kind == LSI:
		if skDef.AttributeName == nil {
			return Index{}, ErrRequiredSortKey
		}
	default:
		return Index{}, ErrUnexpectedIndexKind
	}
	src := []string{kind.String(), *pkDef.AttributeName}
	if skDef.AttributeName != nil {
		src = append(src, *skDef.AttributeName)
	}
	name := joinOmitEmpty(indexNameSep, src...)
	return Index{
		name:  name,
		pkDef: pkDef,
		skDef: skDef,
	}, nil
}

func NewGSI(pk types.AttributeDefinition, skOps ...types.AttributeDefinition) (Index, error) {
	return NewIndex(GSI, pk, skOps...)
}

func MustGSI(pk types.AttributeDefinition, skOps ...types.AttributeDefinition) Index {
	index, err := NewGSI(pk, skOps...)
	if err != nil {
		panic(err)
	}
	return index
}

func NewLSI(pk types.AttributeDefinition, skOps types.AttributeDefinition) (Index, error) {
	return NewIndex(LSI, pk, skOps)
}

func MustLSI(pk types.AttributeDefinition, skOps types.AttributeDefinition) Index {
	index, err := NewLSI(pk, skOps)
	if err != nil {
		panic(err)
	}
	return index
}

func NewByName(name string, pkTyp types.ScalarAttributeType, skTypOps ...types.ScalarAttributeType) (Index, error) {
	if name == "" {
		return Index{}, ErrUnexpectedIndexFormat
	}
	var pkField, skField string
	split := strings.Split(name, indexNameSep)
	switch len(split) {
	case 2:
		pkField = split[1]
	case 3:
		pkField = split[1]
		skField = split[2]
	default:
		return Index{}, ErrUnexpectedIndexFormat
	}
	index := Index{
		name:  name,
		pkDef: types.AttributeDefinition{AttributeName: &pkField, AttributeType: pkTyp},
	}
	if len(skField) > 0 {
		skTyp := types.ScalarAttributeTypeS
		if len(skTypOps) > 0 {
			skTyp = skTypOps[0]
		}
		index.skDef = types.AttributeDefinition{AttributeName: &skField, AttributeType: skTyp}
	}
	return index, nil
}
