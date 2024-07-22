package metrics

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	db "github.com/sayden/streedb"
	"github.com/spaolacci/murmur3"
)

func NewMetric(category, name string, ts int64, val int64) *MetricsEntry {
	return &MetricsEntry{
		MetricName:     name,
		MetricCategory: category,
		Ts:             []int64{ts},
		Val:            []int64{val},
	}
}

type MetricsEntry struct {
	MetricName     string  `parquet:"name=metric_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_LENGTH_BYTE_ARRAY"`
	MetricCategory string  `parquet:"name=metric_category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_LENGTH_BYTE_ARRAY"`
	Ts             []int64 `parquet:"name=ts, type=INT64, encoding=DELTA_BINARY_PACKED, repetitiontype=REPEATED"`
	Val            []int64 `parquet:"name=val, type=INT64, encoding=DELTA_BINARY_PACKED, repetitiontype=REPEATED"`
	min            *int64
	max            *int64
}

func (m *MetricsEntry) Merge(a db.Entry[int64]) error {
	return m.Append(a)
}

func (m *MetricsEntry) Sort() {
	slices.Sort(m.Val)
}

func (m *MetricsEntry) Overlap(min, max int64) (db.Entry[int64], bool) {
	lMin := m.Min()
	lMax := m.Max()

	isOverlapped := (lMin < max || lMin == max) &&
		(min < lMax || min == lMax)
	return m, isOverlapped
}

func (m *MetricsEntry) Append(a db.Entry[int64]) error {
	a_, ok := a.(*MetricsEntry)
	if !ok {
		return errors.New("invalid type")
	}

	m.Val = append(m.Val, a_.Val...)

	m.min = nil
	m.max = nil

	return nil
}

func (m *MetricsEntry) Last() int64 {
	return m.Ts[len(m.Ts)-1]
}

func (m *MetricsEntry) Max() int64 {
	if m.max != nil {
		return *m.max
	}

	max := int64(math.MinInt64)
	for _, v := range m.Ts {
		if v > max {
			max = v
		}
	}

	m.max = &max
	return max
}

func (m *MetricsEntry) Min() int64 {
	if m.min != nil {
		return *m.min
	}
	min := int64(math.MaxInt64)

	for _, v := range m.Ts {
		if v < min {
			min = v
		}
	}

	m.min = &min
	return min
}

func (m *MetricsEntry) LessThan(a db.Comparable[int64]) bool {
	a_ := a.(*MetricsEntry)
	return m.MetricName < a_.MetricName
}

func (m *MetricsEntry) Equals(b db.Comparable[int64]) bool {
	if m.PrimaryIndex() != "" {
		return m.PrimaryIndex() == b.PrimaryIndex() && m.SecondaryIndex() == b.SecondaryIndex()
	}
	return m.SecondaryIndex() == b.SecondaryIndex()
}

func (m *MetricsEntry) SetPrimaryIndex(s string) {
	m.MetricCategory = s
}

func (m *MetricsEntry) PrimaryIndex() string {
	return m.MetricCategory
}

func (m *MetricsEntry) SecondaryIndex() string {
	return m.MetricName
}

func (m *MetricsEntry) Len() int {
	return len(m.Val)
}

func (m *MetricsEntry) UUID() string {
	pIdx := m.PrimaryIndex()
	sIdx := m.SecondaryIndex()
	buf := make([]byte, 0, len(pIdx)+len(sIdx)+8)
	buff := bytes.NewBuffer(buf)
	buff.WriteString(pIdx)
	buff.WriteString(sIdx)
	nBuf := make([]byte, 8)
	n := binary.PutVarint(nBuf, m.Min())
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

func (m *MetricsEntry) IsAdjacent(a db.Comparable[int64]) bool {
	s1 := m.MetricName
	s2 := a.(*MetricsEntry).MetricName

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
