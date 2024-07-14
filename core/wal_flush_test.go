package core

import (
	"cmp"
	"os"
	"testing"
	"time"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/require"
)

type mockFileblockCreator[O cmp.Ordered, E db.Entry[O]] struct {
	newFileblockCount int
	newFileblock      func(es db.EntriesMap[O, E], builder *db.MetadataBuilder[O]) error
}

func (m *mockFileblockCreator[O, E]) NewFileblock(es db.EntriesMap[O, E], builder *db.MetadataBuilder[O]) error {
	m.newFileblockCount++
	return m.newFileblock(es, builder)
}

func TestInMemoryWalFlushStrategy(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("/tmp/db")
	})

	cfg := db.NewDefaultConfig()
	cfg.Wal.MaxItems = 3
	fbcreator := &mockFileblockCreator[int32, *db.Kv]{}

	IWal := newNMMemoryWal(
		cfg,
		fbcreator,
		newItemLimitWalFlushStrategy[int32, *db.Kv](cfg.Wal.MaxItems))

	wal := IWal.(*nmMemoryWal[int32, *db.Kv])

	t.Run("ItemLimitFlushStrategy", func(t *testing.T) {
		fbcreator.newFileblockCount = 0
		wal.flushStrategies = []db.WalFlushStrategy[int32, *db.Kv]{newItemLimitWalFlushStrategy[int32, *db.Kv](cfg.Wal.MaxItems)}

		fbcreator.newFileblock = func(es db.EntriesMap[int32, *db.Kv], builder *db.MetadataBuilder[int32]) error {
			require.Equal(t, 2, es.SecondaryIndicesLen())
			require.Equal(t, 2, len(es.Get("wal_cpu").Val))
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_mem", []int32{3, 4}))
		require.Equal(t, 1, fbcreator.newFileblockCount)

		fbcreator.newFileblock = func(es db.EntriesMap[int32, *db.Kv], builder *db.MetadataBuilder[int32]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			require.Equal(t, 5, len(es.Get("wal_cpu").Val))
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", []int32{5, 6, 7, 8, 9}))
		require.Equal(t, 2, fbcreator.newFileblockCount)
	})

	t.Run("SizeLimitWalFlushStrategy", func(t *testing.T) {
		cfg.Wal.MaxItems = 100
		fbcreator.newFileblockCount = 0
		wal.flushStrategies = []db.WalFlushStrategy[int32, *db.Kv]{
			newSizeLimitWalFlushStrategy[int32, *db.Kv](1000)}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", []int32{3, 4}))
		require.Equal(t, 0, fbcreator.newFileblockCount)

		wal.flushStrategies = []db.WalFlushStrategy[int32, *db.Kv]{
			newSizeLimitWalFlushStrategy[int32, *db.Kv](1)}
		fbcreator.newFileblock = func(es db.EntriesMap[int32, *db.Kv], builder *db.MetadataBuilder[int32]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			require.Equal(t, 7, len(es.Get("wal_cpu").Val))
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", []int32{5, 6, 7}))
		require.Equal(t, 1, fbcreator.newFileblockCount)
	})

	t.Run("TimeLimitWalFlushStrategy", func(t *testing.T) {
		cfg.Wal.MaxItems = 100
		fbcreator.newFileblockCount = 0
		wal.flushStrategies = []db.WalFlushStrategy[int32, *db.Kv]{
			newTimeLimitWalFlushStrategy[int32, *db.Kv](time.Hour)}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", []int32{3, 4}))
		require.Equal(t, 0, fbcreator.newFileblockCount)

		wal.flushStrategies = []db.WalFlushStrategy[int32, *db.Kv]{
			newTimeLimitWalFlushStrategy[int32, *db.Kv](time.Millisecond)}
		fbcreator.newFileblock = func(es db.EntriesMap[int32, *db.Kv], builder *db.MetadataBuilder[int32]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			return nil
		}

		wal.Append(db.NewKv("wal_cpu", "wal_instance1", []int32{5, 6, 7}))
		time.Sleep(time.Millisecond * 50)
		wal.Append(db.NewKv("wal_cpu", "wal_instance1", []int32{5, 6, 7}))

		require.Equal(t, 1, fbcreator.newFileblockCount)
	})
}
