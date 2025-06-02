package dynamox

import (
	"reflect"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func preMarshal(keybase KeyBase) error {
	if hooker, ok := keybase.(KeyBaseHooker); ok {
		return hooker.PreMarshal()
	}
	return nil
}

func PreMarshal(item KeyedItem, skipSaveSK ...bool) error {
	skip := len(skipSaveSK) > 0 && skipSaveSK[0]
	if !skip {
		if err := item.SaveSK(); err != nil {
			return err
		}
	}
	return preMarshal(item.GetKeyBase())
}

func Prepare(item KeyedItem, skipSaveSK ...bool) error {
	return PreMarshal(item, skipSaveSK...)
}

func MarshalMap(item KeyedItem, skipSaveSK ...bool) (map[string]types.AttributeValue, error) {
	if err := PreMarshal(item); err != nil {
		return nil, err
	}
	return attributevalue.MarshalMap(item)
}

func MarshalMapByAny(item any, skipSaveSK ...bool) (map[string]types.AttributeValue, error) {
	switch v := item.(type) {
	case KeyedItem:
		return MarshalMap(v, skipSaveSK...)
	case KeyBase:
		if err := preMarshal(v); err != nil {
			return nil, err
		}
	}
	return attributevalue.MarshalMap(item)
}

func MarshalMapOnlyKey(item KeyedItem, skipSaveSK ...bool) (map[string]types.AttributeValue, error) {
	skip := len(skipSaveSK) > 0 && skipSaveSK[0]
	if !skip {
		if err := item.SaveSK(); err != nil {
			return nil, err
		}
	}
	keybase := item.GetKeyBase()
	if err := preMarshal(keybase); err != nil {
		return nil, err
	}
	return attributevalue.MarshalMap(keybase)
}

func postUnmarshal(keybase KeyBase) error {
	if hooker, ok := keybase.(KeyBaseHooker); ok {
		return hooker.PostUnmarshal()
	}
	return nil
}

func PostUnmarshal(out KeyedItem) error {
	return postUnmarshal(out.GetKeyBase())
}

func UnmarshalMap(m map[string]types.AttributeValue, out KeyedItem) error {
	if err := attributevalue.UnmarshalMap(m, out); err != nil {
		return err
	}
	return PostUnmarshal(out)
}

func UnmarshalMapByAny(m map[string]types.AttributeValue, out any) error {
	switch v := out.(type) {
	case KeyedItem:
		return UnmarshalMap(m, v)
	case KeyBase:
		if err := attributevalue.UnmarshalMap(m, v); err != nil {
			return err
		}
		return postUnmarshal(v)
	default:
		return attributevalue.UnmarshalMap(m, v)
	}
}

func UnmarshalListOfMaps[T KeyedItem](l []map[string]types.AttributeValue, out *[]T) error {
	err := attributevalue.UnmarshalListOfMaps(l, out)
	if err != nil {
		return err
	}
	for _, v := range *out {
		if err = PostUnmarshal(v); err != nil {
			return err
		}
	}
	return nil
}

func UnmarshalListOfMapsByAny(l []map[string]types.AttributeValue, out any) error {
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr {
		return ErrOutMustBePointerToSlice
	}
	sv := rv.Elem()
	if sv.Kind() != reflect.Slice {
		return attributevalue.UnmarshalListOfMaps(l, out)
	}
	if sv.IsNil() {
		sv.Set(reflect.MakeSlice(sv.Type(), 0, len(l)))
	}

	elemTyp := sv.Type().Elem()
	baseTyp := elemTyp
	isPtr := elemTyp.Kind() == reflect.Ptr
	if isPtr {
		baseTyp = elemTyp.Elem()
	}

	for _, elem := range l {
		ref := reflect.New(baseTyp)
		if err := UnmarshalMapByAny(elem, ref.Interface()); err != nil {
			return err
		}

		var value reflect.Value
		if isPtr {
			value = ref
		} else {
			value = ref.Elem()
		}
		sv = reflect.Append(sv, value)
	}
	rv.Elem().Set(sv)
	return nil
}
