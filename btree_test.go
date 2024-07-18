package streedb

import (
	"cmp"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockFilesystem[O cmp.Ordered, E Entry[O]] struct {
	extra struct {
		create               int
		fillMetadataBuilder  int
		load                 int
		openMetaFilesInLevel int
		remove               int
		updateMetadata       int
	}

	es        EntriesMap[O, E]
	builder   *MetadataBuilder[O]
	listeners []FileblockListener[O, E]
}

func (m *mockFilesystem[O, E]) Create(cfg *Config, es EntriesMap[O, E], b *MetadataBuilder[O], ls []FileblockListener[O, E]) (*Fileblock[O, E], error) {
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
func (m *mockFilesystem[O, E]) FillMetadataBuilder(meta *MetadataBuilder[O]) *MetadataBuilder[O] {
	m.extra.fillMetadataBuilder++
	return nil
}
func (m *mockFilesystem[O, E]) Load(*Fileblock[O, E]) (EntriesMap[O, E], error) {
	m.extra.load++
	return nil, nil
}
func (m *mockFilesystem[O, E]) OpenMetaFilesInLevel([]FileblockListener[O, E]) error {
	m.extra.openMetaFilesInLevel++
	return nil
}
func (m *mockFilesystem[O, E]) Remove(*Fileblock[O, E], []FileblockListener[O, E]) error {
	m.extra.remove++
	return nil
}
func (m *mockFilesystem[O, E]) UpdateMetadata(*Fileblock[O, E]) error {
	m.extra.updateMetadata++
	return nil
}

type FIK = Fileblock[int64, *Kv]
type LLF = LinkedList[int64, *FIK]

func createMockFileblock(p, s string, min int64, max int32) *Fileblock[int64, *Kv] {
	kv := NewKv(p, s, []int64{min}, []int32{max})
	meta := &MetaFile[int64]{Min: kv.min, Max: kv.max, PrimaryIdx: p}
	return NewFileblock(nil, meta, &mockFilesystem[int64, *Kv]{})
}

func createMockIndex(t *testing.T) *BtreeWrapper[int64] {
	btree := NewBtreeIndex[int64](3, LLFComp)

	// Build a LLF index Ts=1 with a single kv value: instance1:cpu
	ins_1_cpu_fileblock := createMockFileblock("instance1", "cpu", 1, 4)

	// Insert the LLF in index 1
	btree.Upsert(1, ins_1_cpu_fileblock)

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
	require.Equal(t, ins_1_cpu_fileblock, head)

	require.NotNil(t, ins_1_btree_bis.head.next)
	head = ins_1_btree_bis.head.next.value
	require.NotNil(t, head)
	require.True(t, head != nil && ins_1_mem_fileblock == head)

	return btree
}

func TestBtreeFileblock2(t *testing.T) {
	btree := createMockIndex(t)

	t.Run("Find a range in the tree", func(t *testing.T) {
		// Find the first entry in the btree
		_, found, err := btree.AscendRange("instance1", "cpu", 1, 1)
		require.True(t, found)
		require.Nil(t, err)

		// Find the second entry in the btree
		_, found, err = btree.AscendRange("instance1", "cpu", 2, 2)
		require.False(t, found)
		require.NotNil(t, err)
	})
}
