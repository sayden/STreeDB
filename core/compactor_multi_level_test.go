package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func TestCompactionMultiLevel(t *testing.T) {
	cfg := &db.Config{
		WalMaxItems:      5,
		Filesystem:       db.FilesystemTypeMap[db.FILESYSTEM_TYPE_MEMORY],
		Format:           db.FormatMap[db.FILE_FORMAT_JSON],
		MaxLevels:        2,
		LevelFilesystems: []string{"memory", "memory"},
		DbPath:           "/tmp/db/json",
	}

	mlevel, err := NewLsmTree[db.Integer](cfg)
	assert.NoError(t, err)

	for _, k := range []int{1, 2, 4, 5, 6, 3, 7, 7, 8, 8} {
		mlevel.Append(db.Integer{N: int32(k)})
	}

	err = mlevel.Compact()
	assert.NoError(t, err)

	blocks := mlevel.levels.Level(0).Fileblocks()
	assert.Equal(t, 0, len(blocks))
	blocks = mlevel.levels.Level(1).Fileblocks()
	mergedBlock := blocks[0]
	meta := mergedBlock.Metadata()
	assert.Equal(t, 10, meta.ItemCount)
	es, err := mergedBlock.Load()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(es))
	assert.Equal(t, int32(1), es[0].N)
	assert.Equal(t, int32(8), es[9].N)
}
