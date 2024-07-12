package core

import (
	"os"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
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
	cfg.Compaction.Promoters.ItemLimit.MaxItems = 10

	mlevel, err := NewLsmTree[int32, *db.Kv](cfg)
	if err != nil {
		t.Fatal(err)
	}
	assert.NoError(t, err)

	for _, k := range []int32{1, 2, 4, 5, 6, 3, 7, 8, 9} {
		mlevel.Append(db.NewKv("instance1", []int32{k}, "mem"))
	}
	for _, k := range []int32{1, 2, 4, 5, 6, 3, 7, 7, 1, 2, 4, 5, 6, 3, 7, 7} {
		mlevel.Append(db.NewKv("instance1", []int32{k}, "cpu"))
	}
	for _, k := range []int32{1, 2, 4, 5, 6, 3, 7, 8, 9} {
		mlevel.Append(db.NewKv("instance1", []int32{k}, "cpu"))
	}

	for _, k := range []int32{1, 2, 4, 5, 6, 3, 7} {
		mlevel.Append(db.NewKv("instance2", []int32{k}, "cpu"))
	}
	mlevel.Close()

	err = mlevel.Compact()
	assert.NoError(t, err)

	blocks := mlevel.levels.Level(0).Fileblocks()
	assert.Equal(t, 0, len(blocks))
	blocks = mlevel.levels.Level(1).Fileblocks()
	mergedBlock := blocks[0]
	meta := mergedBlock.Metadata()
	assert.Equal(t, 34, meta.ItemCount)
	es, err := mergedBlock.Load()
	assert.NoError(t, err)
	assert.Equal(t, 10, es.Len())
	assert.Equal(t, int32(1), es.Get(0).Val)
	assert.Equal(t, int32(7), es.Get(9).Val)
}
