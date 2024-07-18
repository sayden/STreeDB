package streedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryIterator(t *testing.T) {
	btree := createMockIndex(t)
	iter := newWrapperIterator[int64, *Kv](btree, 1, 3)

	entry, found, err := iter.Next()
	require.Nil(t, err)
	require.True(t, found)

	fmt.Printf("entry: %v\n", entry)

	entry, found, err = iter.Next()
	require.Nil(t, err)
	require.True(t, found)

	fmt.Printf("entry: %v\n", entry)
}
