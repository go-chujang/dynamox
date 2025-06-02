package dynsa

import "reflect"

func toPtrOrNil[T any](v T) *T {
	if reflect.ValueOf(v).IsZero() {
		return nil
	}
	return &v
}
