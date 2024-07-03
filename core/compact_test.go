package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func TestIsAdjacent(t *testing.T) {
	a := &db.MetaFile[db.Integer]{Min: db.NewInteger(1), Max: db.NewInteger(5)}
	b := &db.MetaFile[db.Integer]{Min: db.NewInteger(6), Max: db.NewInteger(10)}
	c := &db.MetaFile[db.Integer]{Min: db.NewInteger(12), Max: db.NewInteger(15)}

	assert.True(t, isAdjacent(a, b))
	assert.True(t, isAdjacent(b, a))

	assert.False(t, isAdjacent(a, c))
	assert.False(t, isAdjacent(b, c))
	assert.False(t, isAdjacent(c, a))
	assert.False(t, isAdjacent(c, b))

	s1 := &db.MetaFile[db.Entry]{Min: db.NewKv("hello 10", 0), Max: db.NewKv("hello 19", 0)}
	s2 := &db.MetaFile[db.Entry]{Min: db.NewKv("hello 29", 0), Max: db.NewKv("hello 37", 0)}

	assert.False(t, isAdjacent(s1, s2))
}

//
// func TestCompactSameLevel(t *testing.T) {
// 	keys := []int{
// 		1, 2, 4, 5, 6,
// 		3, 7, 7, 8, 8,
// 		10, 11, 12, 13, 14,
// 		15, 11, 17, 18, 19,
// 		20, 21, 22, 23, 24,
// 		25, 26, 16, 27, 28,
// 		29, 44, 45, 36, 59,
// 		60,
// 	}
// 	iter := make([]db.Fileblock[db.Entry], 0)
//
// 	tmpDir := t.TempDir()
// 	defer os.RemoveAll(tmpDir)
//
// 	entries := make(db.Entries[db.Entry], 0, 5)
// 	for i := 0; i < len(keys); i++ {
// 		entries = append(entries, db.Kv{Key: fmt.Sprintf("key %02d", keys[i]), Val: int32(keys[i])})
// 		if i != 0 && i%5 == 0 {
// 			sort.Sort(entries)
// 			meta, err := db.NewMetadataBuilder[db.Entry]().
// 				WithRootPath(tmpDir).
// 				WithEntries(entries).
// 				WithLevel(0).
// 				Build()
// 			assert.NoError(t, err)
//
// 			iter = append(iter, fslocal.NewMemFileblock(entries, 0, meta))
// 			entries = make(db.Entries[db.Entry], 0, 5)
// 		}
// 	}
// 	sort.Ints(keys)
//
// 	cfg := &db.Config{}
// 	memFs := fslocal.NewMemoryFilesystem[db.Entry](cfg)
// 	mock := db.NewBasicLevel(cfg, fslocal.NewMemoryFilesystem[db.Entry](cfg))
// 	singleCompactor := NewSingleLevelCompactor(cfg, memFs, mock)
//
// 	err := singleCompactor.Compact(iter)
// 	assert.NoError(t, err)
// 	mock = db.NewBasicLevel(cfg, fslocal.NewMemoryFilesystem[db.Entry](cfg))
// 	singleCompactor.(*SingleLevelCompactor[db.Entry]).level = mock
//
// 	// assert.Equal(t, 4, len(blocks))
// 	// assert.Equal(t, len(keys), len(blocks[0].(*fslocal.MemFileblock[db.Entry]).Entries)+
// 	// 	len(blocks[1].(*fslocal.MemFileblock[db.Entry]).Entries)+
// 	// 	len(blocks[2].(*fslocal.MemFileblock[db.Entry]).Entries)+
// 	// 	len(blocks[3].(*fslocal.MemFileblock[db.Entry]).Entries))
// 	// blocks2, err := singleCompactor.Compact()
// 	// assert.NoError(t, err)
// 	// assert.NotNil(t, blocks2)
// 	// assert.Equal(t, 3, len(blocks2))
// }
//
// func TestCompactTiered(t *testing.T) {
// 	keys := []int{
// 		1, 2, 4, 5, 6, //0
// 		7, 7, 8, 8, 10, //1
// 		11, 12, 13, 14, 15, //2
// 		15, 11, 17, 18, 19, //3
// 		20, 21, 22, 23, 24, //4
// 		25, 26, 16, 27, 28, //5
// 		30, 44, 45, 36, 59, //6
// 	}
//
// 	iter := make([]db.Fileblock[db.Entry], 0)
// 	entries := make(db.Entries[db.Entry], 0, len(keys))
//
// 	for i := 0; i < len(keys); i++ {
// 		entries = append(entries, db.NewKv(fmt.Sprintf("key %02d", keys[i]), int32(keys[i])))
// 	}
//
// 	tmpDir := t.TempDir()
// 	defer os.RemoveAll(tmpDir)
//
// 	for i := 0; i < len(keys)/5; i++ {
// 		es := entries[i*5 : i*5+5]
// 		sort.Sort(es)
// 		meta, err := db.NewMetadataBuilder[db.Entry]().
// 			WithRootPath(tmpDir).
// 			WithEntries(es).
// 			WithLevel(0).
// 			Build()
// 		assert.NoError(t, err)
//
// 		memblock := fslocal.NewMemFileblock(entries, 0, meta)
// 		iter = append(iter, memblock)
// 	}
//
// 	sort.Ints(keys)
// 	assert.Equal(t, 35, len(keys))
//
// 	cfg := &db.Config{MaxLevels: 5}
// 	levels := db.NewLevels[db.Entry](cfg, fslocal.NewMemoryFilesystem[db.Entry](cfg))
// 	iter[4].Metadata().Level = 1
// 	iter[0].Metadata().Level = 1
//
// 	for _, b := range iter {
// 		levels.AppendFile(b)
// 	}
//
// 	memFs := fslocal.NewMemoryFilesystem[db.Entry](cfg)
//
// 	tieredCompactor := NewTieredSingleFsCompactor(cfg, memFs, levels, NewItemLimitPromoter[db.Entry](cfg, 7))
//
// 	err := tieredCompactor.Compact(iter)
// 	assert.NoError(t, err)
// 	// assert.NotNil(t, newLevels)
// 	//
// 	// assert.Equal(t, 3, len(newLevels.GetLevel(0).Fileblocks()))
// 	// assert.Equal(t, 35, len(newLevels.GetLevel(0).Fileblocks()[0].(*fslocal.MemFileblock[db.Entry]).Entries))
// }
