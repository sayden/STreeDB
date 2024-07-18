package core

import (
	"os"
	"testing"

	db "github.com/sayden/streedb"
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

	require.Equal(t, 0, len(mlevel.levels.Fileblocks()))

	ts := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	mlevel.Append(db.NewKv("instance1", "mem", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 9}))
	require.Equal(t, 0, len(mlevel.levels.Level(0).Fileblocks()))
	require.Equal(t, 0, len(mlevel.levels.Level(1).Fileblocks()))
	mlevel.Append(db.NewKv("instance1", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7, 7, 1, 2, 4, 5, 6, 3, 7, 7}))
	require.Equal(t, 0, len(mlevel.levels.Level(0).Fileblocks()))
	require.Equal(t, 1, len(mlevel.levels.Level(1).Fileblocks()))
	mlevel.Append(db.NewKv("instance1", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7, 8, 11}))
	require.Equal(t, 0, len(mlevel.levels.Level(0).Fileblocks()))
	require.Equal(t, 1, len(mlevel.levels.Level(1).Fileblocks()))
	mlevel.Append(db.NewKv("instance2", "cpu", ts, []int32{1, 2, 4, 5, 6, 3, 7}))
	require.Equal(t, 0, len(mlevel.levels.Level(0).Fileblocks()))
	require.Equal(t, 1, len(mlevel.levels.Level(1).Fileblocks()))

	err = mlevel.Close()
	require.Equal(t, 2, len(mlevel.levels.Level(0).Fileblocks()))
	require.Equal(t, 1, len(mlevel.levels.Level(1).Fileblocks()))
	require.NoError(t, err)

	err = mlevel.Compact()
	require.NoError(t, err)

	blocks := mlevel.levels.Level(0).Fileblocks()
	require.Equal(t, 1, len(blocks))
	blocks = mlevel.levels.Level(1).Fileblocks()
	mergedBlock := blocks[0]
	meta := mergedBlock.Metadata()
	require.Equal(t, 34, meta.ItemCount)
	es, err := mergedBlock.Load()
	require.NoError(t, err)
	require.Equal(t, 2, es.SecondaryIndicesLen())
	kv := es.Get("cpu").(*db.Kv)
	require.Equal(t, 25, len(kv.Val))
	kv = es.Get("mem").(*db.Kv)
	require.Equal(t, 9, len(kv.Val))
}
