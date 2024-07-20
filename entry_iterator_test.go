package streedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thehivecorporation/log"
)

func TestEntryIterator(t *testing.T) {
	log.SetLevel(log.LevelDebug)

	f := func(iter EntryIterator[int64], found bool, err error) {
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
	}

	btree := createMockIndex(t)

	iter, found, err := btree.AscendRange("instance1", "cpu", 1, 8)
	f(iter, found, err)

	iter, found, err = btree.AscendRangeWithFilters(1, 8, PrimaryIndexFilter("instance1"), SecondaryIndexFilter[int64]("cpu"))
	f(iter, found, err)
}
