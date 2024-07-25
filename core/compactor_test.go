package core

import (
	"os"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionMultiLevel(t *testing.T) {
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
	cfg.Compaction.Promoters.ItemLimit.MaxItems = 10

	mlevel, err := NewLsmTree[int64, *db.Kv](cfg)
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, err)

	ts := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	mlevel.Append(db.NewKv("instance1", "mem", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 9}))

	require.Equal(t, 0, countFileblocks(mlevel, 1))

	require.Equal(t, 0, countFileblocks(mlevel, 0))
	require.Equal(t, 0, countFileblocks(mlevel, 1))
	mlevel.Append(db.NewKv("instance1", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7, 7, 1}))
	require.Equal(t, 0, countFileblocks(mlevel, 0))
	require.Equal(t, 1, countFileblocks(mlevel, 1))
	mlevel.Append(db.NewKv("instance1", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 11}))
	require.Equal(t, 0, countFileblocks(mlevel, 0))
	require.Equal(t, 1, countFileblocks(mlevel, 1))
	mlevel.Append(db.NewKv("instance2", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 11}))
	require.Equal(t, 0, countFileblocks(mlevel, 0))
	require.Equal(t, 1, countFileblocks(mlevel, 1))

	err = mlevel.Close()
	require.Equal(t, 2, countFileblocks(mlevel, 0))
	require.Equal(t, 1, countFileblocks(mlevel, 1))
	require.NoError(t, err)

	err = mlevel.Compact()
	require.NoError(t, err)

	blocks := getFileblocksAtLevel(mlevel, 0)
	require.Equal(t, 1, len(blocks))
	blocks = getFileblocksAtLevel(mlevel, 1)
	mergedBlock := blocks[0]
	meta := mergedBlock.Metadata()
	assert.Equal(t, 18, meta.ItemCount)
	es, err := mergedBlock.Load()
	require.NoError(t, err)
	assert.Equal(t, 2, es.SecondaryIndicesLen())
	kv := es.Get("cpu").(*db.Kv)
	assert.Equal(t, 9, len(kv.Val))
	kv = es.Get("mem").(*db.Kv)
	assert.Equal(t, 9, len(kv.Val))
}

func getFileblocksAtLevel(mlevel *LsmTree[int64, *db.Kv], level int) []*db.Fileblock[int64] {
	blocks := make([]*db.Fileblock[int64], 0)

	mlevel.levels.Index.Ascend(func(i *db.BtreeItem[int64]) bool {
		ll := i.Val
		for next, found := ll.Head(); next != nil && found; next = next.Next {
			if next.Val.Metadata().Level == level {
				blocks = append(blocks, next.Val)
			}
		}
		return true
	})

	return blocks
}

func countFileblocks(mlevel *LsmTree[int64, *db.Kv], level int) int {
	count := 0

	mlevel.levels.Index.Ascend(func(i *db.BtreeItem[int64]) bool {
		ll := i.Val
		for next, found := ll.Head(); next != nil && found; next = next.Next {
			if next.Val.Metadata().Level == level {
				count++
			}
		}
		return true
	})

	return count
}
