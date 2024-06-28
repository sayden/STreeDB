package streedb

import "fmt"

type Kv struct {
	Key string `parquet:"name=key, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, repetitiontype=REQUIRED"`
	Val int32  `parquet:"name=val, type=INT32, encoding=DELTA_BINARY_PACKED, repetitiontype=REQUIRED"`
}

func NewLexicographicKv(key string, val int32) *Kv {
	return &Kv{Key: key, Val: val}
}

func (l Kv) LessThan(a Entry) bool {
	a_ := a.(Kv)
	return l.Key < a_.Key
}

func (l Kv) Equals(a Entry) bool {
	a_ := a.(Kv)
	return l.Key == a_.Key
}

func (l Kv) String() string {
	return fmt.Sprintf("'%s'", l.Key)
}
