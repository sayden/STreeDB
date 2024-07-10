package core

import (
	"time"

	db "github.com/sayden/streedb"
)

type memoryWalEntryList[E db.Entry] struct {
	entries db.Entries[E]
}

type nmMemoryWal[E db.Entry] struct {
	entries           map[string]*memoryWalEntryList[E]
	cfg               *db.Config
	fileblockCreator  db.FileblockCreator[E]
	persistStrategies []db.WalFlushStrategy[E]
}

func newNMMemoryWal[E db.Entry](cfg *db.Config, fbc db.FileblockCreator[E], persistStrategies ...db.WalFlushStrategy[E]) db.Wal[E] {
	return &nmMemoryWal[E]{
		entries:           make(map[string]*memoryWalEntryList[E]),
		cfg:               cfg,
		fileblockCreator:  fbc,
		persistStrategies: persistStrategies,
	}
}

func (w *nmMemoryWal[E]) Append(d E) error {
	entryList := w.entries[d.PrimaryIndex()]
	if entryList == nil {
		entryList = &memoryWalEntryList[E]{
			entries: make(db.Entries[E], 0, w.cfg.Wal.MaxItems),
		}
		w.entries[d.PrimaryIndex()] = entryList
	}

	entryList.entries = append(entryList.entries, d)
	for _, s := range w.persistStrategies {
		if s.ShouldFlush(entryList.entries) {
			builder := db.NewMetadataBuilder[E](w.cfg).
				WithLevel(0).
				WithEntries(entryList.entries).
				WithCreatedAt(time.Now())
			err := w.fileblockCreator.NewFileblock(entryList.entries, builder)
			if err != nil {
				return err
			}
			entryList.entries = make(db.Entries[E], 0, w.cfg.Wal.MaxItems)
		}
	}

	return nil
}

// TODO: Implement Find into the WAL
func (w *nmMemoryWal[E]) Find(d E) (E, bool) {
	for _, entries := range w.entries {
		for _, v := range entries.entries {
			if v.Equals(d) {
				return v, true
			}
		}
	}

	return (*new(E)), false
}

func (w *nmMemoryWal[E]) Close() error {
	for _, es := range w.entries {
		if len(es.entries) > 0 {
			builder := db.NewMetadataBuilder[E](w.cfg).
				WithLevel(0).
				WithEntries(es.entries).
				WithCreatedAt(time.Now())
			w.fileblockCreator.NewFileblock(es.entries, builder)
		}
	}
	return nil
}
