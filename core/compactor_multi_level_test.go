package core

import (
	"os"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func TestCompactionMultiLevel(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("/tmp/db/compaction")
	})

	cfg := &db.Config{
		WalMaxItems:      5,
		Format:           db.FormatMap[db.FILE_FORMAT_JSON],
		MaxLevels:        2,
		LevelFilesystems: []string{"local", "local"},
		DbPath:           "/tmp/db/compaction",
	}

	mlevel, err := NewLsmTree[db.Integer](cfg)
	assert.NoError(t, err)

	for _, k := range []int{1, 2, 4, 5, 6, 3, 7, 8, 9} {
		mlevel.Append(db.NewInteger(int32(k), "a", "c"))
	}

	for _, k := range []int{1, 2, 4, 5, 6, 3, 7, 7, 1, 2, 4, 5, 6, 3, 7, 7} {
		mlevel.Append(db.NewInteger(int32(k), "b", "c"))
	}

	err = mlevel.Compact()
	assert.NoError(t, err)

	blocks := mlevel.levels.Level(0).Fileblocks()
	assert.Equal(t, 2, len(blocks))
	blocks = mlevel.levels.Level(1).Fileblocks()
	mergedBlock := blocks[0]
	meta := mergedBlock.Metadata()
	assert.Equal(t, 10, meta.ItemCount)
	es, err := mergedBlock.Load()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(es))
	assert.Equal(t, int32(1), es[0].N)
	assert.Equal(t, int32(7), es[9].N)
}
