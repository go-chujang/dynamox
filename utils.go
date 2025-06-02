package dynamox

import (
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

func KeyedItem2UpdateExpr(item KeyedItem, cond ...expression.ConditionBuilder) (expression.Expression, error) {
	update, err := structToUpdateBuilderOmitEmpty(item, item.PKField(), item.SKField())
	if err != nil {
		return expression.Expression{}, err
	}
	builder := expression.NewBuilder().WithUpdate(update)
	if cond != nil {
		builder = builder.WithCondition(cond[0])
	}
	updateExpr, err := builder.Build()
	if err != nil {
		return expression.Expression{}, err
	}
	return updateExpr, nil
}

func KeyedItem2UpdateBuilder(item KeyedItem) (expression.UpdateBuilder, error) {
	return structToUpdateBuilderOmitEmpty(item, item.PKField(), item.SKField())
}

func IsEnableKey(keyval any, av types.AttributeValue) bool {
	if keyval == nil || av == nil {
		return false
	}
	switch av.(type) {
	case *types.AttributeValueMemberS:
		_, ok := keyval.(string)
		return ok
	case *types.AttributeValueMemberN:
		switch keyval.(type) {
		case int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			float32, float64:
			return true
		default:
			return false
		}
	case *types.AttributeValueMemberB:
		_, ok := keyval.([]byte)
		return ok
	default:
		return false
	}
}

func CheckListOfMaps(av types.AttributeValue) error {
	l, ok := av.(*types.AttributeValueMemberL)
	if !ok {
		return ErrExpectedListAttribute
	}
	for _, v := range l.Value {
		if _, ok := v.(*types.AttributeValueMemberM); !ok {
			return ErrExpectedMapAttribute
		}
	}
	return nil
}

/////////////////////////////////////////////////////////////////////////////

func compositeKey(srcs ...string) string {
	return strings.Join(srcs, CompositeKeySep())
}

var embedCache sync.Map

type pair struct {
	source reflect.Type
	target reflect.Type
}

func indirectType(t reflect.Type) reflect.Type {
	for (t.Kind() == reflect.Ptr || t.Kind() == reflect.Interface) && t.Elem() != nil {
		t = t.Elem()
	}
	return t
}

func hasEmbeddedStruct(v any, targetPtr any) bool {
	if v == nil || targetPtr == nil {
		return false
	}
	src := indirectType(reflect.TypeOf(v))
	tgt := indirectType(reflect.TypeOf(targetPtr))
	if src.Kind() != reflect.Struct || tgt.Kind() != reflect.Struct {
		return false
	}

	key := pair{source: src, target: tgt}
	if val, ok := embedCache.Load(key); ok {
		return val.(bool)
	}

	seen := make(map[reflect.Type]struct{})
	had := hasEmbeddedStruct_r(src, tgt, seen)
	embedCache.Store(key, had)
	return had
}

func hasEmbeddedStruct_r(src, tgt reflect.Type, seen map[reflect.Type]struct{}) bool {
	if src == tgt {
		return true
	}
	if _, ok := seen[src]; ok {
		return false
	}
	seen[src] = struct{}{}

	for i := range src.NumField() {
		field := src.Field(i)
		if !field.Anonymous {
			continue
		}
		fieldType := indirectType(field.Type)
		if fieldType.Kind() != reflect.Struct {
			continue
		}
		if fieldType == tgt {
			return true
		}
		if hasEmbeddedStruct_r(fieldType, tgt, seen) {
			return true
		}
	}
	return false
}

func callMethod(v any, targetPtr any, method string, args ...any) ([]any, error) {
	if !hasEmbeddedStruct(v, targetPtr) {
		return nil, errors.New("target type not embedded")
	}

	rv := reflect.ValueOf(v)
	for (rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface) && !rv.IsNil() {
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil, errors.New("invalid value")
	}

	var mv reflect.Value
	mv = rv.MethodByName(method)
	if !mv.IsValid() {
		if rv.CanAddr() {
			mv = rv.Addr().MethodByName(method)
		}
	}
	if !mv.IsValid() {
		return nil, errors.New("method not found: " + method)
	}

	input := make([]reflect.Value, 0, len(args))
	for _, v := range args {
		input = append(input, reflect.ValueOf(v))
	}

	output := mv.Call(input)
	result := make([]any, len(output))
	for i, v := range output {
		result[i] = v.Interface()
	}
	return result, nil
}

func uuidV4() string  { return uuid.NewString() }
func ulidStr() string { return ulid.Make().String() }

const maxInt = int(^uint(0) >> 1)

func joinOmitEmpty(sep string, s ...string) string {
	switch len(s) {
	case 0:
		return ""
	case 1:
		return s[0]
	}

	var n int
	if len(sep) > 0 {
		if len(sep) >= maxInt/(len(s)-1) {
			panic("strings: Join output length overflow")
		}
		n += len(sep) * (len(s) - 1)
	}
	for _, v := range s {
		if len(v) > maxInt-n {
			panic("strings: Join output length overflow")
		}
		if len(v) > 0 {
			n += len(v)
		}
	}
	var b strings.Builder
	b.Grow(n)
	first := true
	for _, v := range s {
		if len(v) == 0 {
			continue
		}
		if first {
			first = false
		} else {
			b.WriteString(sep)
		}
		b.WriteString(v)
	}
	return b.String()
}

func isNonNilPointer(output any) bool {
	return output != nil && reflect.ValueOf(output).Kind() == reflect.Pointer
}

func isEnableNonUpdateReturnValues(rv types.ReturnValue) bool {
	switch rv {
	case "":
	case types.ReturnValueNone:
	case types.ReturnValueAllOld:
	default:
		return false
	}
	return true
}

func structToUpdateBuilderOmitEmpty(item any, omitKeyOps ...string) (expression.UpdateBuilder, error) {
	if item == nil {
		return expression.UpdateBuilder{}, ErrEmptyForUpdate
	}
	rv := reflect.ValueOf(item)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		rv = reflect.Indirect(rv)
	}
	if rv.IsZero() || rv.Type().Kind() != reflect.Struct {
		return expression.UpdateBuilder{}, ErrEmptyForUpdate
	}

	omitKeys := make(map[string]struct{}, len(omitKeyOps))
	for _, key := range omitKeyOps {
		omitKeys[key] = struct{}{}
	}
	update, count := structToUpdateBuilderOmitEmpty_r(rv, omitKeys)
	if count == 0 {
		return expression.UpdateBuilder{}, ErrEmptyForUpdate
	}
	return update, nil
}

func structToUpdateBuilderOmitEmpty_r(rv reflect.Value, omitKeys map[string]struct{}) (update expression.UpdateBuilder, count int) {
	for i := range rv.Type().NumField() {
		value := rv.Field(i)
		if value.IsZero() {
			continue
		}
		dynamoField := strings.TrimSuffix(rv.Type().Field(i).Tag.Get("dynamodbav"), ",omitempty")
		if dynamoField == "-" {
			continue
		}
		if _, exist := omitKeys[dynamoField]; exist {
			continue
		}
		if value.Kind() == reflect.Struct {
			n := 0
			update, n = structToUpdateBuilderOmitEmpty_r(value, omitKeys)
			count += n
		} else {
			if dynamoField == "" {
				continue
			}
			update = update.Set(expression.Name(dynamoField), expression.Value(value.Interface()))
			count++
		}
	}
	return update, count
}
