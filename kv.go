package streedb

import (
	"fmt"
	"time"
)

func NewKv(key string, val int32, primaryIdx string) Kv {
	return Kv{Key: key, Val: val, PrimaryIdx: primaryIdx, createdAt: time.Now()}
}

type Kv struct {
	createdAt  time.Time
	PrimaryIdx string
	Key        string `parquet:"name=key, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, repetitiontype=REQUIRED"`
	Val        int32  `parquet:"name=val, type=INT32, encoding=DELTA_BINARY_PACKED, repetitiontype=REQUIRED"`
}

func (l Kv) LessThan(a Entry) bool {
	a_ := a.(Kv)
	return l.Key < a_.Key
}

func (l Kv) Equals(b Entry) bool {
	if l.PrimaryIndex() != "" {
		return l.PrimaryIndex() == b.PrimaryIndex() && l.SecondaryIndex() == b.SecondaryIndex()
	}
	return l.SecondaryIndex() == b.SecondaryIndex()
}

func (l Kv) PrimaryIndex() string {
	return l.PrimaryIdx
}

func (l Kv) SecondaryIndex() string {
	return l.Key
}

func (l Kv) Adjacent(a Entry) bool {
	s1 := l.Key
	s2 := a.(Kv).Key

	// If the strings are empty or have different lengths, they're not adjacent
	if len(s1) == 0 || len(s2) == 0 || len(s1) != len(s2) {
		return false
	}

	// Compare all characters except the last one
	for i := 0; i < len(s1)-1; i++ {
		if s1[i] != s2[i] {
			return false
		}
	}

	// Check if the last characters are adjacent
	lastChar1 := s1[len(s1)-1]
	lastChar2 := s2[len(s2)-1]

	// Check if the last characters are consecutive letters or digits
	if (lastChar1 >= 'a' && lastChar1 <= 'z' && lastChar2 >= 'a' && lastChar2 <= 'z') ||
		(lastChar1 >= 'A' && lastChar1 <= 'Z' && lastChar2 >= 'A' && lastChar2 <= 'Z') ||
		(lastChar1 >= '0' && lastChar1 <= '9' && lastChar2 >= '0' && lastChar2 <= '9') {
		return abs(int(lastChar1)-int(lastChar2)) == 1
	}

	return false
}

// Helper function to calculate absolute value
func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func (l Kv) String() string {
	return fmt.Sprintf("'%s'", l.Key)
}

func (l Kv) CreationTime() time.Time {
	return l.createdAt
}
