package streedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntryFallsInside(t *testing.T) {
	v := NewKv("hello", []int32{12, 15}, "hello 15")
	a := NewKv("hello", []int32{10, 16}, "hello 16")
	b := NewKv("hello", []int32{25, 28}, "hello 28")

	assert.True(t, EntryFallsInsideMinMax(a.Min(), a.Max(), v.Min()))
	assert.False(t, EntryFallsInsideMinMax(b.Min(), b.Max(), v.Min()))
}

func TestEntries(t *testing.T) {
	temp := make([]*Kv, 0)
	es := NewSliceToMap(temp)
	val := NewKv("hello", []int32{15}, "hello 15")
	es.Append(val)
	first := es.Get(0)
	assert.Equal(t, "hello", first.Key)
	assert.Equal(t, "hello 15", first.PrimaryIdx)
}
