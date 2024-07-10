package streedb

import (
	"math"
	"time"
)

func NewInteger(n int32, p, s string) Integer {
	return Integer{N: n, primaryIdx: p, secondaryIdx: s, createdAt: time.Now()}
}

type Integer struct {
	primaryIdx   string
	secondaryIdx string
	createdAt    time.Time
	N            int32 `parquet:"name=n, type=INT32"`
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

func IntegerCmp(a, b Integer) int {
	if a.N < b.N {
		return -1
	}
	if a.N > b.N {
		return 1
	}

	return 0
}

func (i Integer) Cmp(a, b Entry) int {
	if a.(Integer).N < b.(Integer).N {
		return -1
	}
	if a.(Integer).N > b.(Integer).N {
		return 1
	}
	return 0
}

func (i Integer) PrimaryIndex() string {
	return i.primaryIdx
}

func (i Integer) SecondaryIndex() string {
	return i.secondaryIdx
}

func (i Integer) CreationTime() time.Time {
	return i.createdAt
}
