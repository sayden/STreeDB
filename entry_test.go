package streedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntryFallsInside(t *testing.T) {
	v := NewKv("hello", "hello 15", []int64{12, 15}, []int32{12, 15})
	a := NewKv("hello", "hello 16", []int64{10, 16}, []int32{10, 16})
	b := NewKv("hello", "hello 28", []int64{25, 28}, []int32{25, 28})

	assert.True(t, EntryFallsInsideMinMax(a.Min(), a.Max(), v.Min()))
	assert.False(t, EntryFallsInsideMinMax(b.Min(), b.Max(), v.Min()))
}

func TestEntries(t *testing.T) {
	temp := make([]*Kv, 0)
	em := NewSliceToMap(temp)
	val := NewKv("hello", "hello 15", []int64{1}, []int32{15})
	em.Append(val)
	first := em.Get("hello 15")
	require.NotNil(t, first)
	assert.Equal(t, "hello", first.PrimaryIndex())
	assert.Equal(t, "hello 15", first.SecondaryIndex())
}
