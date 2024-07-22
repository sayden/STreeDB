package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/require"
)

func TestWal(t *testing.T) {
	cfg := db.NewDefaultConfig()

	fbc := &mockFileblockCreator[int64]{}
	wal := newNMMemoryWal[int64](cfg, fbc, newItemLimitWalFlushStrategy[int64](cfg.Wal.MaxItems))
	require.NotNil(t, wal)

	err := wal.Append(db.NewKv("hello", "hello 15", []int64{11, 12, 13, 14, 15}, []int32{1, 2, 3, 4, 5}))
	require.NoError(t, err)

	testTable := []struct {
		pIdx  string
		sIdx  string
		min   int64
		max   int64
		found bool
	}{
		{"hello", "hello 15", 12, 14, true},
		{"hello", "hello 15", 11, 14, true},
		{"hello", "hello 15", 10, 14, true},
		{"hello", "hello 15", 7, 9, false},
		{"hello", "hello 15", 13, 18, true},
		{"hello", "hello 15", 19, 21, false},
		{"hello1", "hello 15", 11, 14, false},
		{"hello", "hello 13", 11, 14, false},
	}

	iter, found := wal.Find("hello", "hello 15", 12, 14)
	require.True(t, found)
	entry, found, err := iter.Next()
	require.NoError(t, err)
	require.True(t, found)

	kv, ok := entry.(*db.Kv)
	require.True(t, ok)

	require.NotNil(t, kv)
	require.Equal(t, "hello", kv.PrimaryIndex())
	require.Equal(t, "hello 15", kv.SecondaryIndex())
	require.Equal(t, []int64{11, 12, 13, 14, 15}, kv.Ts)
	require.Equal(t, []int32{1, 2, 3, 4, 5}, kv.Val)

	for _, tt := range testTable {
		_, found := wal.Find(tt.pIdx, tt.sIdx, tt.min, tt.max)
		require.Equal(t, tt.found, found)
	}
}
