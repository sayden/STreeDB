package core

import (
	"os"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionAdvanced(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("/tmp/db/compaction") })

	defaultCfg := db.NewDefaultConfig()

	cfg := &db.Config{
		Wal:              defaultCfg.Wal,
		Filesystem:       db.FilesystemTypeMap[db.FILESYSTEM_TYPE_LOCAL],
		MaxLevels:        2,
		LevelFilesystems: []string{"local", "local"},
		DbPath:           "/tmp/db/compaction",
		Compaction:       defaultCfg.Compaction,
	}
	cfg.Wal.MaxItems = 10
	cfg.Compaction.Promoters.ItemLimit.FirstBlockItemCount = 11
	cfg.Compaction.Promoters.ItemLimit.GrowthFactor = 2

	mlevel, err := NewLsmTree[int64, *db.Kv](cfg)
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, err)
	wal, ok := mlevel.wal.(*memoryWal[int64])
	require.True(t, ok)

	ts := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}

	// Add the first 9 items, no flushing from wal, no new blocks created
	mlevel.Append(db.NewKv("instance1", "mem", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 9}))
	assert.Equal(t, 0, countFileblocks(mlevel, 0))
	assert.Equal(t, 0, countFileblocks(mlevel, 1))
	walItems := countWalItems(wal)
	assert.Equal(t, 9, walItems)

	// Add 9 more to the same primary index. A flush is triggered with the contents of the wal
	mlevel.Append(db.NewKv("instance1", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 10}))
	assert.Equal(t, 0, countFileblocks(mlevel, 0))
	assert.Equal(t, 1, countFileblocks(mlevel, 1))
	walItems = countWalItems(wal)
	assert.Equal(t, 0, walItems)
	iter, found := wal.Find("instance1", "cpu", 0, 4)
	assert.False(t, found)
	assert.Nil(t, iter)
	iter, found, err = mlevel.levels.FindSingle("instance1", "", 0, 4)
	require.NoError(t, err)
	assert.True(t, found)
	count := 0
	var entry db.Entry[int64]
	for entry, found, err = iter.Next(); found && entry != nil && err == nil; entry, found, err = iter.Next() {
		count += entry.Len()
	}
	assert.Equal(t, 18, count)
	if err != nil {
		t.Fatal(err)
	}

	// Add 9 items to wal, no flushing because the previous flush was successful and the wal has
	// been emptied in during the flushing
	mlevel.Append(db.NewKv("instance1", "cpu", []int64{10, 11, 12, 13, 14, 15, 16, 17, 18}, []int32{1, 2, 4, 5, 6, 3, 7, 8, 11}))
	assert.Equal(t, 0, countFileblocks(mlevel, 0))
	assert.Equal(t, 1, countFileblocks(mlevel, 1))
	walItems = countWalItems(wal)
	assert.Equal(t, 9, walItems)

	// Add 9 items to a different primary index (`instance2`). No WAL flushing, total wal items is
	// 18 now, over the defined limit of `cfg.Wal.MaxItems = 10`
	mlevel.Append(db.NewKv("instance2", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 11}))
	assert.Equal(t, 0, countFileblocks(mlevel, 0))
	assert.Equal(t, 1, countFileblocks(mlevel, 1))
	walItems = countWalItems(wal)
	assert.Equal(t, 18, walItems)
	// t.Logf("Wal items: %d", walItems)

	// When closing, a WAL flushing is forced, so 2 new blocks, one for `instance1` and one for
	// `instance2` must be created with the 18 items that were still left on the WAL
	err = mlevel.Close()
	assert.Equal(t, 2, countFileblocks(mlevel, 0))
	assert.Equal(t, 1, countFileblocks(mlevel, 1))
	require.NoError(t, err)

	// Compaction must merge all blocks from 'instance1' into a single block, only 1 block more
	// must be left
	err = mlevel.Compact()
	require.NoError(t, err)

	// Check block at level 0
	l0Blocks := getFileblocksAtLevel(mlevel, 0)
	require.Equal(t, 1, len(l0Blocks))
	_0MergedBlock := l0Blocks[0]
	meta := _0MergedBlock.Metadata()
	assert.Equal(t, 0, meta.Level)
	assert.Equal(t, 9, meta.ItemCount)
	assert.Equal(t, "instance2", meta.PrimaryIdx)
	require.Equal(t, 1, len(meta.Rows))
	_0Row := meta.Rows[0]
	assert.Equal(t, "cpu", _0Row.SecondaryIdx)
	assert.Equal(t, 9, _0Row.ItemCount)
	assert.Equal(t, int64(1), _0Row.Min)
	assert.Equal(t, int64(9), _0Row.Max)

	// Check block at level 1
	l1Blocks := getFileblocksAtLevel(mlevel, 1)
	require.Equal(t, 1, len(l1Blocks))
	_1MergedBlock := l1Blocks[0]
	meta = _1MergedBlock.Metadata()
	assert.Equal(t, 1, meta.Level)
	assert.Equal(t, 27, meta.ItemCount)
	es, err := _1MergedBlock.Load()
	require.NoError(t, err)
	assert.Equal(t, 2, es.SecondaryIndicesLen())
	kv := es.Get("cpu").(*db.Kv)
	assert.Equal(t, 18, len(kv.Val))
	kv = es.Get("mem").(*db.Kv)
	assert.Equal(t, 9, len(kv.Val))
}
