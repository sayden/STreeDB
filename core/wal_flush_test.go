package core

import (
	"cmp"
	"os"
	"testing"
	"time"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/require"
)

type mockFileblockCreator[O cmp.Ordered] struct {
	newFileblockCount int
	newFileblock      func(es *db.EntriesMap[O], builder *db.MetadataBuilder[O]) error
}

func (m *mockFileblockCreator[O]) NewFileblock(es *db.EntriesMap[O], builder *db.MetadataBuilder[O]) error {
	m.newFileblockCount++
	return m.newFileblock(es, builder)
}

func TestInMemoryWalFlushStrategy(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("/tmp/db") })

	cfg := db.NewDefaultConfig()
	cfg.Wal.MaxItems = 3
	fbcreator := &mockFileblockCreator[int64]{}

	now := time.Now().UnixMilli()
	ts := []int64{now, now + 1, now + 2, now + 3}
	ts3 := ts[:3]
	ts2 := ts[:2]

	t.Run("ItemLimitFlushStrategy", func(t *testing.T) {
		IWal := newNMMemoryWal(
			cfg,
			fbcreator,
			newItemLimitWalFlushStrategy[int64](cfg.Wal.MaxItems))

		wal := IWal.(*nmMemoryWal[int64])

		fbcreator.newFileblockCount = 0
		wal.flushStrategies = []db.WalFlushStrategy[int64]{newItemLimitWalFlushStrategy[int64](cfg.Wal.MaxItems)}

		fbcreator.newFileblock = func(es *db.EntriesMap[int64], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 2, es.SecondaryIndicesLen())
			require.Equal(t, 2, es.Get("wal_cpu").Len())
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_mem", ts2, []int32{3, 4}))
		require.Equal(t, 1, fbcreator.newFileblockCount)

		fbcreator.newFileblock = func(es *db.EntriesMap[int64], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			require.Equal(t, 4, es.Get("wal_cpu").Len())
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts, []int32{5, 6, 7, 8, 9}))
		require.Equal(t, 2, fbcreator.newFileblockCount)
	})

	t.Run("SizeLimitWalFlushStrategy", func(t *testing.T) {
		IWal := newNMMemoryWal(
			cfg,
			fbcreator,
			newItemLimitWalFlushStrategy[int64](cfg.Wal.MaxItems))

		wal := IWal.(*nmMemoryWal[int64])

		cfg.Wal.MaxItems = 100
		fbcreator.newFileblockCount = 0
		wal.flushStrategies = []db.WalFlushStrategy[int64]{
			newSizeLimitWalFlushStrategy[int64](1300)}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{3, 4}))
		require.Equal(t, 0, fbcreator.newFileblockCount)

		wal.flushStrategies = []db.WalFlushStrategy[int64]{
			newSizeLimitWalFlushStrategy[int64](1)}
		fbcreator.newFileblock = func(es *db.EntriesMap[int64], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			require.Equal(t, 7, es.Get("wal_cpu").Len())
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts3, []int32{5, 6, 7}))
		require.Equal(t, 1, fbcreator.newFileblockCount)
	})

	t.Run("TimeLimitWalFlushStrategy", func(t *testing.T) {
		t.Skip("TODO")

		IWal := newNMMemoryWal(
			cfg,
			fbcreator,
			newTimeLimitWalFlushStrategy(time.Millisecond))

		wal := IWal.(*nmMemoryWal[int64])

		cfg.Wal.MaxItems = 100
		fbcreator.newFileblockCount = 0
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts[2:], []int32{3, 4}))
		require.Equal(t, 0, fbcreator.newFileblockCount)

		wal.flushStrategies = []db.WalFlushStrategy[int64]{
			newTimeLimitWalFlushStrategy(time.Millisecond),
		}
		fbcreator.newFileblock = func(es *db.EntriesMap[int64], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			return nil
		}

		wal.Append(db.NewKv("wal_cpu", "wal_instance1", ts3, []int32{5, 6, 7}))
		time.Sleep(time.Millisecond * 500)
		wal.Append(db.NewKv("wal_cpu", "wal_instance1", ts3, []int32{5, 6, 7}))

		require.Equal(t, 1, fbcreator.newFileblockCount)
	})
}
