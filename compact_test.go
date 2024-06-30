package streedb

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockLevel[T Entry] struct {
	resultEntries    Entries[T]
	iter             []Fileblock[T]
	currentEntries   int
	currentFileblock int
}

func (c *mockLevel[T]) Load() (Entries[T], error) {
	res := c.iter[c.currentEntries]
	c.currentEntries++
	res2 := res.(*MemFileblock[T])

	return res2.entries, nil
}

func (c *mockLevel[T]) Fileblocks() []Fileblock[T]                 { return c.iter }
func (c *mockLevel[T]) Find(v Entry) (Entry, bool, error)          { return nil, false, nil }
func (c *mockLevel[T]) Merge(a Fileblock[T]) (Fileblock[T], error) { return nil, nil }
func (c *mockLevel[T]) Close() error                               { return nil }
func (c *mockLevel[T]) Remove() error                              { return nil }
func (c *mockLevel[T]) AppendFile(b Fileblock[T])                  {}
func (c *mockLevel[T]) RemoveFiles(r map[int]struct{})             {}

func mockBuilder[T Entry]() FileblockBuilder[T] {
	return func(entries Entries[T]) (Fileblock[T], error) {
		return &MemFileblock[T]{
			entries: entries,
		}, nil
	}
}

type MemFileblock[T Entry] struct {
	entries  Entries[T]
	metadata *MetaFile[T]
}

func (m *MemFileblock[T]) Metadata() *MetaFile[T] {
	return &MetaFile[T]{
		Min: m.entries[0],
		Max: m.entries[len(m.entries)-1],
	}
}

func (m *MemFileblock[T]) Load() (Entries[T], error)                  { return m.entries, nil }
func (m *MemFileblock[T]) Find(v Entry) (Entry, bool, error)          { return nil, false, nil }
func (m *MemFileblock[T]) Merge(a Fileblock[T]) (Fileblock[T], error) { return nil, nil }
func (m *MemFileblock[T]) Close() error                               { return nil }
func (m *MemFileblock[T]) Remove() error                              { return nil }

func TestIsAdjacent(t *testing.T) {
	a := &MetaFile[Integer]{Min: NewInteger(1), Max: NewInteger(5)}
	b := &MetaFile[Integer]{Min: NewInteger(6), Max: NewInteger(10)}
	c := &MetaFile[Integer]{Min: NewInteger(12), Max: NewInteger(15)}

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
	iter := make([]Fileblock[Entry], 0)

	entries := make(Entries[Entry], 0, 5)
	for i := 0; i < len(keys); i++ {
		entries = append(entries, Kv{Key: fmt.Sprintf("key %02d", keys[i]), Val: int32(keys[i])})
		if i != 0 && i%5 == 0 {
			sort.Sort(entries)
			iter = append(iter, &MemFileblock[Entry]{entries: entries})
			entries = make(Entries[Entry], 0, 5)
		}
	}
	sort.Ints(keys)

	sameCompactor := SameLevelCompactor[Entry]{
		fBuilder: mockBuilder[Entry](),
		level: &mockLevel[Entry]{
			iter: iter,
		},
	}

	blocks, err := sameCompactor.Compact()
	assert.NoError(t, err)
	sameCompactor.level = &mockLevel[Entry]{iter: blocks}

	assert.Equal(t, 2, len(blocks))
	assert.Equal(t, len(keys), len(blocks[0].(*MemFileblock[Entry]).entries)+len(blocks[1].(*MemFileblock[Entry]).entries))
	blocks2, err := sameCompactor.Compact()
	assert.NoError(t, err)
	assert.NotNil(t, blocks2)
	assert.Equal(t, 2, len(blocks2))
	for i, v := range keys[:31] {
		assert.Equal(t, int32(v), blocks[1].(*MemFileblock[Entry]).entries[i].(Kv).Val)
	}
	for i, v := range keys[31:] {
		assert.Equal(t, int32(v), blocks[0].(*MemFileblock[Entry]).entries[i].(Kv).Val)
	}

}
