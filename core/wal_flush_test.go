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
	t.Cleanup(func() { os.RemoveAll("/tmp/db") })

	cfg := db.NewDefaultConfig()
	cfg.Wal.MaxItems = 3
	fbcreator := &mockFileblockCreator[int64, *db.Kv]{}

	now := time.Now().UnixMilli()
	ts := []int64{now, now + 1, now + 2, now + 3}
	ts3 := ts[:3]
	ts2 := ts[:2]

	t.Run("ItemLimitFlushStrategy", func(t *testing.T) {
		IWal := newNMMemoryWal(
			cfg,
			fbcreator,
			newItemLimitWalFlushStrategy[int64, *db.Kv](cfg.Wal.MaxItems))

		wal := IWal.(*nmMemoryWal[int64, *db.Kv])

		fbcreator.newFileblockCount = 0
		wal.flushStrategies = []db.WalFlushStrategy[int64, *db.Kv]{newItemLimitWalFlushStrategy[int64, *db.Kv](cfg.Wal.MaxItems)}

		fbcreator.newFileblock = func(es db.EntriesMap[int64, *db.Kv], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 2, es.SecondaryIndicesLen())
			require.Equal(t, 2, len(es.Get("wal_cpu").Val))
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_mem", ts2, []int32{3, 4}))
		require.Equal(t, 1, fbcreator.newFileblockCount)

		fbcreator.newFileblock = func(es db.EntriesMap[int64, *db.Kv], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			require.Equal(t, 5, len(es.Get("wal_cpu").Val))
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts, []int32{5, 6, 7, 8, 9}))
		require.Equal(t, 2, fbcreator.newFileblockCount)
	})

	t.Run("SizeLimitWalFlushStrategy", func(t *testing.T) {
		IWal := newNMMemoryWal(
			cfg,
			fbcreator,
			newItemLimitWalFlushStrategy[int64, *db.Kv](cfg.Wal.MaxItems))

		wal := IWal.(*nmMemoryWal[int64, *db.Kv])

		cfg.Wal.MaxItems = 100
		fbcreator.newFileblockCount = 0
		wal.flushStrategies = []db.WalFlushStrategy[int64, *db.Kv]{
			newSizeLimitWalFlushStrategy[int64, *db.Kv](1300)}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{3, 4}))
		require.Equal(t, 0, fbcreator.newFileblockCount)

		wal.flushStrategies = []db.WalFlushStrategy[int64, *db.Kv]{
			newSizeLimitWalFlushStrategy[int64, *db.Kv](1)}
		fbcreator.newFileblock = func(es db.EntriesMap[int64, *db.Kv], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			require.Equal(t, 7, len(es.Get("wal_cpu").Val))
			return nil
		}
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts3, []int32{5, 6, 7}))
		require.Equal(t, 1, fbcreator.newFileblockCount)
	})

	t.Run("TimeLimitWalFlushStrategy", func(t *testing.T) {
		IWal := newNMMemoryWal(
			cfg,
			fbcreator,
			newTimeLimitWalFlushStrategy[*db.Kv](time.Millisecond))

		wal := IWal.(*nmMemoryWal[int64, *db.Kv])

		cfg.Wal.MaxItems = 100
		fbcreator.newFileblockCount = 0
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts2, []int32{1, 2}))
		wal.Append(db.NewKv("wal_instance1", "wal_cpu", ts[2:], []int32{3, 4}))
		require.Equal(t, 0, fbcreator.newFileblockCount)

		wal.flushStrategies = []db.WalFlushStrategy[int64, *db.Kv]{
			newTimeLimitWalFlushStrategy[*db.Kv](time.Millisecond),
		}
		fbcreator.newFileblock = func(es db.EntriesMap[int64, *db.Kv], builder *db.MetadataBuilder[int64]) error {
			require.Equal(t, 1, es.SecondaryIndicesLen())
			return nil
		}

		wal.Append(db.NewKv("wal_cpu", "wal_instance1", ts3, []int32{5, 6, 7}))
		time.Sleep(time.Millisecond * 50)
		wal.Append(db.NewKv("wal_cpu", "wal_instance1", ts3, []int32{5, 6, 7}))

		require.Equal(t, 1, fbcreator.newFileblockCount)
	})
}
