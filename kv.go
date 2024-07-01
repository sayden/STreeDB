package streedb

import (
	"fmt"
	"math"
)

type Kv struct {
	Key string `parquet:"name=key, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, repetitiontype=REQUIRED"`
	Val int32  `parquet:"name=val, type=INT32, encoding=DELTA_BINARY_PACKED, repetitiontype=REQUIRED"`
}

func NewKv(key string, val int32) Kv {
	return Kv{Key: key, Val: val}
}

func (l Kv) LessThan(a Entry) bool {
	a_ := a.(Kv)
	return l.Key < a_.Key
}

func (l Kv) Equals(a Entry) bool {
	a_ := a.(Kv)
	return l.Key == a_.Key
}

func (l Kv) Adjacent(a Entry) bool {
	s1 := l.Key
	s2 := a.(Kv).Key

	// TODO: Different length strings aren't considered Adjacent
	// Probably I have to think this again
	if math.Abs(float64(len(s1)-len(s2))) > 0 {
		return false
	}

	j := 0
	for i := 0; i < len(s1); i++ {
		if s1[i] == s2[j] {
			j++
			continue
		}
	}

	return false
}

func (l Kv) String() string {
	return fmt.Sprintf("'%s'", l.Key)
}
