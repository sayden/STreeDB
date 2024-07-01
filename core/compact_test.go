package core

import (
	"fmt"
	"sort"
	"testing"

	"github.com/sayden/streedb"
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
	res2 := res.(*MemFileblock[T])

	return res2.entries, nil
}

func (c *mockLevel[T]) Fileblocks() []streedb.Fileblock[T]                         { return c.iter }
func (c *mockLevel[T]) Find(v streedb.Entry) (streedb.Entry, bool, error)          { return nil, false, nil }
func (c *mockLevel[T]) Merge(a streedb.Fileblock[T]) (streedb.Fileblock[T], error) { return nil, nil }
func (c *mockLevel[T]) Close() error                                               { return nil }
func (c *mockLevel[T]) Remove() error                                              { return nil }
func (c *mockLevel[T]) AppendFile(b streedb.Fileblock[T])                          {}
func (c *mockLevel[T]) RemoveFiles(r map[int]struct{})                             {}

func mockBuilder[T streedb.Entry]() streedb.FileblockBuilder[T] {
	return func(entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
		return &MemFileblock[T]{
			entries: entries,
			metadata: streedb.MetaFile[T]{
				Min:   entries[0],
				Max:   entries[len(entries)-1],
				Level: level,
			},
		}, nil
	}
}

type MemFileblock[T streedb.Entry] struct {
	entries  streedb.Entries[T]
	metadata streedb.MetaFile[T]
}

func (m *MemFileblock[T]) Metadata() *streedb.MetaFile[T] {
	m.metadata.Min = m.entries[0]
	m.metadata.Max = m.entries[len(m.entries)-1]
	return &m.metadata
}

func (m *MemFileblock[T]) Load() (streedb.Entries[T], error) { return m.entries, nil }
func (m *MemFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	return nil, false, nil
}
func (m *MemFileblock[T]) Merge(a streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	return nil, nil
}
func (m *MemFileblock[T]) Close() error  { return nil }
func (m *MemFileblock[T]) Remove() error { return nil }

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
}

func TestCompactSameLevel(t *testing.T) {
	keys := []int{
		1, 2, 4, 5, 6, 3, 7, 7, 8, 8,
		10, 11, 12, 13, 14, 15, 11, 17,
		18, 19, 20, 21, 22, 23, 24, 25,
		26, 16, 27, 28, 29, 44, 45, 36,
		59, 60,
	}
	iter := make([]streedb.Fileblock[streedb.Entry], 0)

	entries := make(streedb.Entries[streedb.Entry], 0, 5)
	for i := 0; i < len(keys); i++ {
		entries = append(entries, streedb.Kv{Key: fmt.Sprintf("key %02d", keys[i]), Val: int32(keys[i])})
		if i != 0 && i%5 == 0 {
			sort.Sort(entries)
			iter = append(iter, &MemFileblock[streedb.Entry]{entries: entries})
			entries = make(streedb.Entries[streedb.Entry], 0, 5)
		}
	}
	sort.Ints(keys)

	sameCompactor := SameLevelCompactor[streedb.Entry]{
		fBuilder: mockBuilder[streedb.Entry](),
		level:    &mockLevel[streedb.Entry]{iter: iter},
	}

	blocks, err := sameCompactor.Compact()
	assert.NoError(t, err)
	mock := &mockLevel[streedb.Entry]{iter: blocks}
	sameCompactor.level = mock

	assert.Equal(t, 2, len(blocks))
	assert.Equal(t, len(keys), len(blocks[0].(*MemFileblock[streedb.Entry]).entries)+len(blocks[1].(*MemFileblock[streedb.Entry]).entries))
	blocks2, err := sameCompactor.Compact()
	assert.NoError(t, err)
	assert.NotNil(t, blocks2)
	assert.Equal(t, 2, len(blocks2))
	for i, v := range keys[:31] {
		assert.Equal(t, int32(v), blocks[1].(*MemFileblock[streedb.Entry]).entries[i].(streedb.Kv).Val)
	}
	for i, v := range keys[31:] {
		assert.Equal(t, int32(v), blocks[0].(*MemFileblock[streedb.Entry]).entries[i].(streedb.Kv).Val)
	}

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

	for i := 0; i < len(keys)/5; i++ {
		es := entries[i*5 : i*5+5]
		sort.Sort(es)
		iter = append(iter, &MemFileblock[streedb.Entry]{entries: es})
	}

	sort.Ints(keys)
	assert.Equal(t, 35, len(keys))

	levels := streedb.NewLevels[streedb.Entry](&streedb.Config{}).(streedb.BasicLevels[streedb.Entry])
	levels[0] = iter[1:4]
	levels[1] = append(levels[1], iter[4])
	levels[1] = append(levels[1], iter[0])
	levels[0] = append(levels[0], iter[5])
	levels[0] = append(levels[0], iter[6])

	// set the level in metadata to all level 1's
	for i := 0; i < len(levels[1]); i++ {
		levels[1][i].Metadata().Level = 1
	}

	assert.Equal(t, 5, len(levels[0]))
	assert.Equal(t, 2, len(levels[1]))

	tieredCompactor := TieredCompactor[streedb.Entry]{
		fBuilder: mockBuilder[streedb.Entry](),
		levels:   levels,
		cfg:      &streedb.Config{MaxLevels: 5},
	}
	_ = tieredCompactor

	newLevels, err := tieredCompactor.Compact()
	assert.NoError(t, err)
	assert.NotNil(t, newLevels)

	assert.Equal(t, 1, len(newLevels.GetLevel(0)))
	assert.Equal(t, 5, len(newLevels.GetLevel(0)[0].(*MemFileblock[streedb.Entry]).entries))
	assert.Equal(t, 2, len(newLevels.GetLevel(1)))
	assert.Equal(t, 5, len(newLevels.GetLevel(1)[0].(*MemFileblock[streedb.Entry]).entries))
}
