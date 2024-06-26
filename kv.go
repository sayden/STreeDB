package main

type LexicographicKv struct {
	Key string `parquet:"name=key, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, repetitiontype=REQUIRED"`
	Val int32  `parquet:"name=val, type=INT32, encoding=DELTA_BINARY_PACKED, repetitiontype=REQUIRED"`
}

func NewLexicographicKv(key string, val int32) *LexicographicKv {
	return &LexicographicKv{Key: key, Val: val}
}

func (l LexicographicKv) LessThan(a Entry) bool {
	a_ := a.(LexicographicKv)
	return l.Key < a_.Key
}

func (l LexicographicKv) Equals(a Entry) bool {
	a_ := a.(LexicographicKv)
	return l.Key == a_.Key
}
