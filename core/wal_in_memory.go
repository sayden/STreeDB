package core

import (
	"cmp"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	db "github.com/sayden/streedb"
)

func newNMMemoryWal[O cmp.Ordered](cfg *db.Config, fbc db.FileblockCreator[O], persistStrategies ...db.WalFlushStrategy[O]) db.Wal[O] {
	return &memoryWal[O]{
		entries:          xsync.NewMapOf[string, *db.EntriesMap[O]](),
		cfg:              cfg,
		fileblockCreator: fbc,
		flushStrategies:  persistStrategies,
	}
}

// memoryWal is a write-ahead log that stores entries in memory.
// That means that the entries are not persisted to disk until the
// a persist strategies is met or the WAL is closed (usually when
// closing the database)
type memoryWal[O cmp.Ordered] struct {
	entries          *xsync.MapOf[string, *db.EntriesMap[O]]
	cfg              *db.Config
	fileblockCreator db.FileblockCreator[O]
	flushStrategies  []db.WalFlushStrategy[O]
}

func (w *memoryWal[O]) Append(d db.Entry[O]) (err error) {
	fileEntries, _ := w.entries.LoadOrStore(d.PrimaryIndex(), db.NewEntriesMap[O]())
	fileEntries.Append(d)

	for _, strategy := range w.flushStrategies {
		if strategy.ShouldFlush(fileEntries) {
			builder := db.NewMetadataBuilder[O](w.cfg).
				WithPrimaryIndex(d.PrimaryIndex()).
				WithLevel(0).
				WithCreatedAt(time.Now())

			if err = w.fileblockCreator.NewFileblock(fileEntries, builder); err != nil {
				return err
			}

			w.entries.Delete(d.PrimaryIndex())

			return nil
		}
	}

	return nil
}

func (w *memoryWal[O]) Find(pIdx, sIdx string, min, max O) (db.EntryIterator[O], bool) {
	if pIdx == "" {
		entries := make([]db.Entry[O], 0)
		w.entries.Range(func(key string, fileEntries *db.EntriesMap[O]) bool {
			fileEntries.Range(func(key string, entry db.Entry[O]) bool {
				if sIdx == "" || entry.SecondaryIndex() == sIdx {
					entry.Sort()
					entries = append(entries, entry)
				}
				return true
			})
			return true
		})

		return db.NewListIterator(entries), len(entries) > 0
	}

	fileEntries, found := w.entries.Load(pIdx)
	if !found {
		return nil, false
	}

	return fileEntries.Find(sIdx, min, max)
}

func (w *memoryWal[O]) Close() error {
	var err error
	w.entries.Range(func(key string, fileEntries *db.EntriesMap[O]) bool {
		// TODO: I don't think that this can actually happen
		pIdx := fileEntries.PrimaryIndex()
		if pIdx == "" {
			panic("unreachable")
		}

		builder := db.NewMetadataBuilder[O](w.cfg).
			WithLevel(0).
			WithPrimaryIndex(pIdx).
			WithCreatedAt(time.Now())

		if err = w.fileblockCreator.NewFileblock(fileEntries, builder); err != nil {
			return false
		}
		return true
	})

	return err
}
