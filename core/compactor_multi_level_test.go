package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
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

	promoter := NewItemLimitPromoter[db.Integer](7, 2)
	mlevel, err := fs.NewMultiFsLevels(cfg, promoter)
	assert.NoError(t, err)

	es0 := db.Entries[db.Integer]{}
	for _, k := range []int{1, 2, 4, 5, 6} {
		es0 = append(es0, db.Integer{N: int32(k)})
	}

	es1 := db.Entries[db.Integer]{}
	for _, k := range []int{3, 7, 7, 8, 8} {
		es1 = append(es1, db.Integer{N: int32(k)})
	}

	err = mlevel.Create(es0, 0)
	assert.NoError(t, err)
	err = mlevel.Create(es1, 0)
	assert.NoError(t, err)

	compactor, err := NewTieredMultiFsCompactor(cfg, mlevel)
	assert.NoError(t, err)

	blocks := mlevel.GetLevel(0).Fileblocks()

	err = compactor.Compact(blocks)
	assert.NoError(t, err)

	blocks = mlevel.GetLevel(0).Fileblocks()
	assert.Equal(t, 0, len(blocks))

	blocks = mlevel.GetLevel(1).Fileblocks()
	assert.Equal(t, 1, len(blocks))
	mergedBlock := blocks[0]
	meta := mergedBlock.Metadata()
	assert.Equal(t, 10, meta.ItemCount)
	es, err := mergedBlock.Load()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(es))
	assert.Equal(t, int32(1), es[0].N)
	assert.Equal(t, int32(8), es[9].N)
}
