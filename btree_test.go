package streedb

import (
	"cmp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFilesystem[O cmp.Ordered] struct {
	emap  *EntriesMap[O]
	extra struct {
		create               int
		fillMetadataBuilder  int
		load                 int
		openMetaFilesInLevel int
		remove               int
		updateMetadata       int
	}

	es        *EntriesMap[O]
	builder   *MetadataBuilder[O]
	listeners []FileblockListener[O]
}

func (m *mockFilesystem[O]) Create(cfg *Config, es *EntriesMap[O], b *MetadataBuilder[O], ls []FileblockListener[O]) (*Fileblock[O], error) {
	m.es = es
	m.builder = b
	m.listeners = ls
	m.extra.create++

	meta, err := b.Build()
	if err != nil {
		return nil, err
	}

	return NewFileblock(cfg, meta, m), nil
}
func (m *mockFilesystem[O]) FillMetadataBuilder(meta *MetadataBuilder[O]) *MetadataBuilder[O] {
	m.extra.fillMetadataBuilder++
	return nil
}
func (m *mockFilesystem[O]) Load(*Fileblock[O]) (*EntriesMap[O], error) {
	m.extra.load++
	return m.emap, nil
}
func (m *mockFilesystem[O]) OpenMetaFilesInLevel([]FileblockListener[O]) error {
	m.extra.openMetaFilesInLevel++
	return nil
}
func (m *mockFilesystem[O]) Remove(*Fileblock[O], []FileblockListener[O]) error {
	m.extra.remove++
	return nil
}
func (m *mockFilesystem[O]) UpdateMetadata(*Fileblock[O]) error {
	m.extra.updateMetadata++
	return nil
}

type FIK = Fileblock[int64]
type LLF = LinkedList[int64, *FIK]

func createMockFileblock(p, s string, min int64, max int64) *Fileblock[int64] {
	meta := &MetaFile[int64]{
		ItemCount:  1,
		Min:        &min,
		Max:        &max,
		PrimaryIdx: p,
		Rows: []Row[int64]{
			{
				SecondaryIdx: s,
				Min:          min,
				Max:          max,
				ItemCount:    1,
			},
		},
	}

	emap := NewEntriesMap[int64]()
	emap.Store(s, &Kv{
		Ts:         []int64{1},
		Val:        []int32{1},
		max:        &max,
		min:        &min,
		Key:        s,
		PrimaryIdx: p,
	})
	return NewFileblock(nil, meta, &mockFilesystem[int64]{emap: emap})
}

func createMockIndex(t *testing.T) *BtreeIndex[int64, int64] {
	btree := NewBtreeIndex[int64, int64](3, LLFComp)

	// Build a LLF index Ts=1 with a single kv value: instance1:cpu
	ins_1_cpu_fileblock := createMockFileblock("instance1", "cpu", 1, 4)
	ins_1_cpu_fileblock2 := createMockFileblock("instance1", "cpu", 5, 9)

	// Insert the LLF in index 1
	btree.Upsert(1, ins_1_cpu_fileblock)
	btree.Upsert(5, ins_1_cpu_fileblock2)

	// Build a LLF index Ts=2 with a single kv value: instance2:cpu
	ins_2_fileblock := createMockFileblock("instance2", "cpu", 2, 4)

	// Insert the LLF in index 2
	found := btree.Upsert(2, ins_2_fileblock)
	require.False(t, found)

	// Build a LLF index Ts=1 with a single kv value: instance1:mem. This entry receives ins_1_llf
	// because there is already an existing llf for Ts=1 so, after receiving it, we must update it
	// and re-insert it
	ins_1_mem_fileblock := createMockFileblock("instance1", "mem", 1, 3)

	// When inserting now, the btree should return the already existing ll in the btree.
	found = btree.Upsert(1, ins_1_mem_fileblock)
	require.True(t, found)

	// Retrieve the LLF from the btree and check that the two Fileblocks exists
	ins_1_btree_bis, found := btree.Get(1)
	require.True(t, found && ins_1_btree_bis != nil)

	head, found := ins_1_btree_bis.Head()
	require.True(t, found && head != nil)
	require.Equal(t, ins_1_cpu_fileblock, head.Val)

	require.NotNil(t, ins_1_btree_bis.head.Next)
	head = ins_1_btree_bis.head.Next
	require.NotNil(t, head)
	require.True(t, head != nil && ins_1_mem_fileblock == head.Val)

	return btree
}

func TestBtreeFileblock2(t *testing.T) {
	btree := createMockIndex(t)

	// Find the first entry in the btree
	_, found, err := btree.ascendRange("instance1", "cpu", 1, 2)
	assert.True(t, found)
	require.Nil(t, err)

	// Find the second entry in the btree
	data, found, err := btree.ascendRange("instance1", "cpu", 2, 2)
	assert.False(t, found)
	assert.Nil(t, err)
	assert.Empty(t, data)
}
