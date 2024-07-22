package streedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/spaolacci/murmur3"
)

func NewKv(primaryIdx, secondary string, ts []int64, val []int32) *Kv {
	return &Kv{
		PrimaryIdx: primaryIdx,
		Key:        secondary,
		Val:        val,
		Ts:         ts,
	}
}

type Kv struct {
	PrimaryIdx string
	Ts         []int64 `parquet:"name=ts, type=INT64, encoding=DELTA_BINARY_PACKED, repetitiontype=REPEATED"`
	Key        string  `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_LENGTH_BYTE_ARRAY"`
	Val        []int32 `parquet:"name=val, type=INT32, encoding=DELTA_BINARY_PACKED, repetitiontype=REPEATED"`
	min        *int64
	max        *int64
}

func (l *Kv) Merge(a Entry[int64]) error {
	return l.Append(a)
}

func (l *Kv) Sort() {
	if sort.IsSorted(l) {
		return
	}

	sort.Sort(l)
}

func (l *Kv) Len() int {
	return len(l.Ts)
}

func (l *Kv) Less(i, j int) bool {
	return l.Ts[i] < l.Ts[j]
}

func (l *Kv) Swap(i, j int) {
	l.Ts[i], l.Ts[j] = l.Ts[j], l.Ts[i]
	l.Val[i], l.Val[j] = l.Val[j], l.Val[i]
}

func (l *Kv) Overlap(min, max int64) (Entry[int64], bool) {
	lMin := l.Min()
	lMax := l.Max()

	isOverlapped := (lMin < max || lMin == max) &&
		(min < lMax || min == lMax)

	return l, isOverlapped
}

func (l *Kv) Append(a Entry[int64]) error {
	a_, ok := a.(*Kv)
	if !ok {
		return errors.New("invalid type")
	}

	l.Ts = append(l.Ts, a_.Ts...)
	l.Val = append(l.Val, a_.Val...)

	l.min = nil
	l.max = nil

	return nil
}

func (l *Kv) Last() int64 {
	return l.Ts[len(l.Ts)-1]
}

func (l *Kv) Max() int64 {
	if l.max != nil {
		return *l.max
	}

	max := int64(math.MinInt64)
	for _, v := range l.Ts {
		if v > max {
			max = v
		}
	}

	l.max = &max
	return max
}

func (l *Kv) Min() int64 {
	if l.min != nil {
		return *l.min
	}
	min := int64(math.MaxInt64)

	for _, v := range l.Ts {
		if v < min {
			min = v
		}
	}

	l.min = &min
	return min
}

func (l *Kv) LessThan(a Comparable[int64]) bool {
	a_ := a.(*Kv)
	return l.Key < a_.Key
}

func (l *Kv) Equals(b Comparable[int64]) bool {
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

func (l *Kv) UUID() string {
	pIdx := l.PrimaryIndex()
	sIdx := l.SecondaryIndex()
	buf := make([]byte, 0, len(pIdx)+len(sIdx)+8)
	buff := bytes.NewBuffer(buf)
	buff.WriteString(pIdx)
	buff.WriteString(sIdx)
	nBuf := make([]byte, 8)
	n := binary.PutVarint(nBuf, l.Min())
	if n != 8 {
		panic("unexpected amount of bytes written")
	}
	buff.Write(nBuf)

	hash, err := murmur3.New64().Write(buff.Bytes())
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%d", hash)
}

func (l *Kv) IsAdjacent(a Comparable[int64]) bool {
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

func (l *Kv) String() string {
	return fmt.Sprintf("'%s'", l.Key)
}

// Helper function to calculate absolute value
func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
