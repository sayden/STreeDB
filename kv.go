package streedb

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

func NewKv(primaryIdx, secondary string, val []int32) *Kv {
	return &Kv{
		PrimaryIdx: primaryIdx,
		Key:        secondary,
		Val:        val,
		createdAt:  time.Now(),
	}
}

type Kv struct {
	PrimaryIdx string
	createdAt  time.Time
	Key        string  `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_LENGTH_BYTE_ARRAY"`
	Val        []int32 `parquet:"name=val, type=INT32, encoding=DELTA_BINARY_PACKED, repetitiontype=REPEATED"`
}

func (l *Kv) Merge(a Entry[int32]) error {
	a_, ok := a.(*Kv)
	if !ok {
		return errors.New("invalid type")
	}

	l.Val = append(l.Val, a_.Val...)
	slices.Sort(l.Val)

	return nil
}

func (l *Kv) Append(a Entry[int32]) error {
	a_, ok := a.(*Kv)
	if !ok {
		return errors.New("invalid type")
	}

	l.Val = append(l.Val, a_.Val...)

	return nil
}

func (l *Kv) Get(i int) any {
	return l.Val[i]
}

func (l *Kv) Last() int32 {
	return l.Val[len(l.Val)-1]
}

func (l *Kv) Max() int32 {
	max := int32(math.MinInt32)
	for _, v := range l.Val {
		if v > max {
			max = v
		}
	}
	return max
}

func (l *Kv) Min() int32 {
	min := int32(math.MaxInt32)

	for _, v := range l.Val {
		if v < min {
			min = v
		}
	}

	return min
}

func (l *Kv) LessThan(a Comparable[int32]) bool {
	a_ := a.(*Kv)
	return l.Key < a_.Key
}

func (l *Kv) Equals(b Comparable[int32]) bool {
	if l.PrimaryIndex() != "" {
		return l.PrimaryIndex() == b.PrimaryIndex() && l.SecondaryIndex() == b.SecondaryIndex()
	}
	return l.SecondaryIndex() == b.SecondaryIndex()
}

func (l *Kv) SetPrimaryIndex(s string) {
	l.PrimaryIdx = s
}

func (l *Kv) PrimaryIndex() string {
	return l.PrimaryIdx
}

func (l *Kv) SecondaryIndex() string {
	return l.Key
}

func (l *Kv) Len() int {
	return len(l.Val)
}

func (l *Kv) Adjacent(a Comparable[int32]) bool {
	s1 := l.Key
	s2 := a.(*Kv).Key

	// If the strings are empty or have different lenggths, they're not adjacent
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

func (l *Kv) String() string {
	return fmt.Sprintf("'%s'", l.Key)
}

func (l Kv) CreationTime() time.Time {
	return l.createdAt
}
