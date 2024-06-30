package streedb

import "math"

func NewInteger(n int32) Integer {
	return Integer{N: n}
}

type Integer struct {
	N int32 `parquet:"name=n, type=INT32"`
}

func (i Integer) LessThan(a Entry) bool {
	return i.N < a.(Integer).N
}

func (i Integer) Equals(a Entry) bool {
	return i.N == a.(Integer).N
}

func (i Integer) Adjacent(a Entry) bool {
	a_ := a.(Integer).N
	b_ := i.N
	fl := float64(b_ - a_)

	res := math.Abs(fl) == 1
	return res
}
