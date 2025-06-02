package dynamox

import (
	"sync/atomic"
	"time"
)

type timestampUnit int

const (
	Seconds      timestampUnit = iota
	MilliSeconds               // default
	MicroSeconds
	NanoSeconds

	defaultCompositeKeySeperator = "#"
)

var (
	newTimestamp    atomic.Value
	parseTimestamp  atomic.Value
	compositeKeySep atomic.Value
)

func init() {
	SetTimestampUnit(MilliSeconds)
	SetCompositeKeySep(defaultCompositeKeySeperator)
}

func SetTimestampUnit(tsu timestampUnit) {
	switch tsu {
	case Seconds:
		newTimestamp.Store(func() int64 { return time.Now().Unix() })
		parseTimestamp.Store(func(sec int64) time.Time { return time.Unix(sec, 0) })
	case MicroSeconds:
		newTimestamp.Store(func() int64 { return time.Now().UnixMicro() })
		parseTimestamp.Store(func(usec int64) time.Time { return time.Unix(usec/1e6, (usec%1e6)*1e3) })
	case NanoSeconds:
		newTimestamp.Store(func() int64 { return time.Now().UnixNano() })
		parseTimestamp.Store(func(nsec int64) time.Time { return time.Unix(0, nsec) })
	default: // MilliSeconds
		newTimestamp.Store(func() int64 { return time.Now().UnixMilli() })
		parseTimestamp.Store(func(msec int64) time.Time { return time.Unix(msec/1e3, (msec%1e3)*1e6) })
	}
}

func SetCompositeKeySep(sep string) {
	if len(sep) > 0 {
		compositeKeySep.Store(sep)
	}
}

func newTimestampFn() int64 {
	fn := newTimestamp.Load().(func() int64)
	return fn()
}

func parseTimestampFn(ts int64) time.Time {
	fn := parseTimestamp.Load().(func(int64) time.Time)
	return fn(ts)
}

func CompositeKeySep() string {
	return compositeKeySep.Load().(string)
}
