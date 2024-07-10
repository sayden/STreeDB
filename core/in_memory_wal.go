package core

import (
	"time"

	db "github.com/sayden/streedb"
)

type inMemoryWal[E db.Entry] struct {
	entries           db.Entries[E]
	cfg               *db.Config
	fileblockCreator  db.FileblockCreator[E]
	persistStrategies []db.WalFlushStrategy[E]
}

func newInMemoryWal[E db.Entry](c *db.Config, fbc db.FileblockCreator[E], persistStrategies ...db.WalFlushStrategy[E]) db.Wal[E] {
	return &inMemoryWal[E]{
		entries:           make(db.Entries[E], 0, c.Wal.MaxItems),
		cfg:               c,
		fileblockCreator:  fbc,
		persistStrategies: persistStrategies,
	}
}

func (w *inMemoryWal[E]) Append(d E) error {
	w.entries = append(w.entries, d)

	for _, s := range w.persistStrategies {
		if s.ShouldFlush(w.entries) {
			builder := db.NewMetadataBuilder[E](w.cfg).
				WithLevel(0).
				WithEntries(w.entries).
				WithCreatedAt(time.Now())
			err := w.fileblockCreator.NewFileblock(w.entries, builder)
			if err != nil {
				return err
			}
			w.entries = make(db.Entries[E], 0, w.cfg.Wal.MaxItems)
		}
	}

	return nil
}

func (w *inMemoryWal[E]) Find(d E) (E, bool) {
	for _, v := range w.entries {
		if v.Equals(d) {
			return v, true
		}
	}

	return (*new(E)), false
}

func (w *inMemoryWal[E]) Close() error {
	if len(w.entries) > 0 {
		builder := db.NewMetadataBuilder[E](w.cfg).
			WithLevel(0).
			WithEntries(w.entries).
			WithCreatedAt(time.Now())
		w.fileblockCreator.NewFileblock(w.entries, builder)
	}

	return nil
}

func (w *inMemoryWal[E]) Data() db.Entries[E] {
	return w.entries
}
