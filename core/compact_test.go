package core

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/sayden/streedb"
	fslocal "github.com/sayden/streedb/fs/local"
	"github.com/stretchr/testify/assert"
)

type mockLevel[T streedb.Entry] struct {
	resultEntries    streedb.Entries[T]
	iter             []streedb.Fileblock[T]
	currentEntries   int
	currentFileblock int
}

func (c *mockLevel[T]) Load() (streedb.Entries[T], error) {
	res := c.iter[c.currentEntries]
	c.currentEntries++
	res2 := res.(*fslocal.MemFileblock[T])

	return res2.Entries, nil
}

func (c *mockLevel[T]) Fileblocks() []streedb.Fileblock[T]                       { return c.iter }
func (c *mockLevel[T]) Find(v streedb.Entry) (streedb.Entry, bool, error)        { return nil, false, nil }
func (c *mockLevel[T]) Merge(a streedb.Fileblock[T]) (streedb.Entries[T], error) { return nil, nil }
func (c *mockLevel[T]) Close() error                                             { return nil }
func (c *mockLevel[T]) Remove() error                                            { return nil }
func (c *mockLevel[T]) AppendFile(b streedb.Fileblock[T])                        {}
func (c *mockLevel[T]) RemoveFile(b streedb.Fileblock[T]) error                  { return nil }

func TestIsAdjacent(t *testing.T) {
	a := &streedb.MetaFile[streedb.Integer]{Min: streedb.NewInteger(1), Max: streedb.NewInteger(5)}
	b := &streedb.MetaFile[streedb.Integer]{Min: streedb.NewInteger(6), Max: streedb.NewInteger(10)}
	c := &streedb.MetaFile[streedb.Integer]{Min: streedb.NewInteger(12), Max: streedb.NewInteger(15)}

	assert.True(t, isAdjacent(a, b))
	assert.True(t, isAdjacent(b, a))

	assert.False(t, isAdjacent(a, c))
	assert.False(t, isAdjacent(b, c))
	assert.False(t, isAdjacent(c, a))
	assert.False(t, isAdjacent(c, b))

	s1 := &streedb.MetaFile[streedb.Entry]{Min: streedb.NewKv("hello 10", 0), Max: streedb.NewKv("hello 19", 0)}
	s2 := &streedb.MetaFile[streedb.Entry]{Min: streedb.NewKv("hello 29", 0), Max: streedb.NewKv("hello 37", 0)}

	assert.False(t, isAdjacent(s1, s2))
}

func TestCompactSameLevel(t *testing.T) {
	keys := []int{
		1, 2, 4, 5, 6,
		3, 7, 7, 8, 8,
		10, 11, 12, 13, 14,
		15, 11, 17, 18, 19,
		20, 21, 22, 23, 24,
		25, 26, 16, 27, 28,
		29, 44, 45, 36, 59,
		60,
	}
	iter := make([]streedb.Fileblock[streedb.Entry], 0)

	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	entries := make(streedb.Entries[streedb.Entry], 0, 5)
	for i := 0; i < len(keys); i++ {
		entries = append(entries, streedb.Kv{Key: fmt.Sprintf("key %02d", keys[i]), Val: int32(keys[i])})
		if i != 0 && i%5 == 0 {
			sort.Sort(entries)
			meta, err := streedb.NewMetadataBuilder[streedb.Entry](tmpDir).
				WithEntries(entries).
				WithLevel(0).
				Build()
			assert.NoError(t, err)

			iter = append(iter, fslocal.NewMemFileblock(entries, 0, meta))
			entries = make(streedb.Entries[streedb.Entry], 0, 5)
		}
	}
	sort.Ints(keys)

	cfg := &streedb.Config{}
	memFs := fslocal.NewMemoryFilesystem[streedb.Entry](cfg)
	mock := &mockLevel[streedb.Entry]{iter: iter}
	singleCompactor := NewSingleLevelCompactor[streedb.Entry](cfg, memFs, mock)

	blocks, err := singleCompactor.Compact()
	assert.NoError(t, err)
	mock = &mockLevel[streedb.Entry]{iter: blocks}
	singleCompactor.(*SingleLevelCompactor[streedb.Entry]).level = mock

	assert.Equal(t, 4, len(blocks))
	assert.Equal(t, len(keys), len(blocks[0].(*fslocal.MemFileblock[streedb.Entry]).Entries)+
		len(blocks[1].(*fslocal.MemFileblock[streedb.Entry]).Entries)+
		len(blocks[2].(*fslocal.MemFileblock[streedb.Entry]).Entries)+
		len(blocks[3].(*fslocal.MemFileblock[streedb.Entry]).Entries))
	blocks2, err := singleCompactor.Compact()
	assert.NoError(t, err)
	assert.NotNil(t, blocks2)
	assert.Equal(t, 3, len(blocks2))
}

func TestCompactTiered(t *testing.T) {
	keys := []int{
		1, 2, 4, 5, 6, //0
		7, 7, 8, 8, 10, //1
		11, 12, 13, 14, 15, //2
		15, 11, 17, 18, 19, //3
		20, 21, 22, 23, 24, //4
		25, 26, 16, 27, 28, //5
		30, 44, 45, 36, 59, //6
	}

	iter := make([]streedb.Fileblock[streedb.Entry], 0)
	entries := make(streedb.Entries[streedb.Entry], 0, len(keys))

	for i := 0; i < len(keys); i++ {
		entries = append(entries, streedb.NewKv(fmt.Sprintf("key %02d", keys[i]), int32(keys[i])))
	}

	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	for i := 0; i < len(keys)/5; i++ {
		es := entries[i*5 : i*5+5]
		sort.Sort(es)
		meta, err := streedb.NewMetadataBuilder[streedb.Entry](tmpDir).
			WithEntries(es).
			WithLevel(0).
			Build()
		assert.NoError(t, err)

		memblock := fslocal.NewMemFileblock(entries, 0, meta)
		iter = append(iter, memblock)
	}

	sort.Ints(keys)
	assert.Equal(t, 35, len(keys))

	cfg := &streedb.Config{MaxLevels: 5}
	levels := streedb.NewLevels[streedb.Entry](cfg, fslocal.NewMemoryFilesystem[streedb.Entry](cfg))
	iter[4].Metadata().Level = 1
	iter[0].Metadata().Level = 1

	for _, b := range iter {
		levels.AppendFile(b)
	}

	memFs := fslocal.NewMemoryFilesystem[streedb.Entry](cfg)

	tieredCompactor := NewTieredCompactor(cfg, memFs, levels, NewItemLimitPromoter(cfg, memFs, 7))

	newLevels, err := tieredCompactor.Compact()
	assert.NoError(t, err)
	assert.NotNil(t, newLevels)

	assert.Equal(t, 3, len(newLevels.GetLevel(0).Fileblocks()))
	assert.Equal(t, 35, len(newLevels.GetLevel(0).Fileblocks()[0].(*fslocal.MemFileblock[streedb.Entry]).Entries))
}
