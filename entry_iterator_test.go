package streedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thehivecorporation/log"
)

func TestEntryIterator(t *testing.T) {
	log.SetLevel(log.LevelDebug)

	btree := createMockIndex(t)

	iter, found, err := btree.AscendRangeWithFilters(1, 8, PrimaryIndexFilter("instance1"), SecondaryIndexFilter[int64]("cpu"))
	assert.True(t, found)
	require.Nil(t, err)

	entry, found, err := iter.Next()
	assert.True(t, found)
	require.Nil(t, err)
	require.NotNil(t, entry)

	// entry is an instance1:cpu entry
	assert.Equal(t, "instance1", entry.PrimaryIndex())
	assert.Equal(t, "cpu", entry.SecondaryIndex())
	assert.Equal(t, int64(1), entry.Min())
	assert.Equal(t, int64(4), entry.Max())
	assert.Equal(t, 1, len(entry.(*Kv).Ts))

	entry, found, err = iter.Next()
	require.Nil(t, err)
	require.True(t, found)

	// entry is, again, an instance1:cpu entry, second kv from the test dataset
	assert.Equal(t, "instance1", entry.PrimaryIndex())
	assert.Equal(t, "cpu", entry.SecondaryIndex())
	assert.Equal(t, int64(5), entry.Min())
	assert.Equal(t, int64(9), entry.Max())

	// By just requesting the instance1:cpu entries, we should get a total of 3 entries: 2 for cpu and 1 for mem
	// But the btree actually contains 4 entries, one extra for instance2:cpu
	iter, found, err = btree.AscendRangeWithFilters(1, 8, PrimaryIndexFilter("instance1"))
	assert.True(t, found)
	require.Nil(t, err)

	cpuTotal := 0
	memTotal := 0
	instance1Total := 0
	instance2Total := 0

	counterSecondary := func(entry Entry[int64]) {
		if entry.SecondaryIndex() == "cpu" {
			cpuTotal++
		} else {
			memTotal++
		}
	}
	counterPrimary := func(entry Entry[int64]) {
		if entry.PrimaryIndex() == "instance1" {
			instance1Total++
		} else {
			instance2Total++
		}
	}

	for i := 0; i < 4; i++ {
		entry, found, err = iter.Next()
		assert.True(t, found)
		require.Nil(t, err)
		require.NotNil(t, entry)
		assert.True(t, entry.SecondaryIndex() == "cpu" || entry.SecondaryIndex() == "mem")
		counterSecondary(entry)
	}

	assert.Equal(t, 2, cpuTotal)
	assert.Equal(t, 1, memTotal)

	entry, found, err = iter.Next()
	assert.False(t, found)
	assert.Nil(t, err)
	assert.Nil(t, entry)

	// By just requesting the cpu entries, we should get a total of 3 entries: 2 for instance1 and 1 for instance2
	// But the btree actually contains 4 entries, one extra for instance1:mem
	iter, found, err = btree.AscendRangeWithFilters(1, 8, SecondaryIndexFilter[int64]("cpu"))
	assert.True(t, found)
	require.Nil(t, err)
	cpuTotal = 0
	memTotal = 0

	f := func(entry Entry[int64]) {
		assert.True(t, found)
		require.Nil(t, err)
		require.NotNil(t, entry)
		counterSecondary(entry)
		counterPrimary(entry)
	}

	entry, found, err = iter.Next()
	f(entry)
	assert.Equal(t, "cpu", entry.SecondaryIndex())
	assert.True(t, entry.PrimaryIndex() == "instance1" || entry.PrimaryIndex() == "instance2")

	entry, found, err = iter.Next()
	f(entry)
	assert.Equal(t, "cpu", entry.SecondaryIndex())
	assert.True(t, entry.PrimaryIndex() == "instance1" || entry.PrimaryIndex() == "instance2")

	entry, found, err = iter.Next()
	f(entry)
	assert.Equal(t, "cpu", entry.SecondaryIndex())
	assert.True(t, entry.PrimaryIndex() == "instance1" || entry.PrimaryIndex() == "instance2")

	assert.Equal(t, 3, cpuTotal)
	assert.Equal(t, 0, memTotal)
	assert.Equal(t, 2, instance1Total)
	assert.Equal(t, 1, instance2Total)

	iter, found, err = btree.AscendRangeWithFilters(1, 8)
	assert.True(t, found)
	require.Nil(t, err)
	cpuTotal = 0
	memTotal = 0

	for i := 0; i < 4; i++ {
		entry, found, err = iter.Next()
		f(entry)
		assert.True(t, entry.SecondaryIndex() == "cpu" || entry.SecondaryIndex() == "mem")
		assert.True(t, entry.PrimaryIndex() == "instance1" || entry.PrimaryIndex() == "instance2")
	}

	entry, found, err = iter.Next()
	assert.False(t, found)
	assert.Nil(t, err)
	assert.Nil(t, entry)
}
