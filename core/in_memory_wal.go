package core

import (
	"time"

	db "github.com/sayden/streedb"
)

type inMemoryWal[E db.Entry] struct {
	entries          db.Entries[E]
	cfg              *db.Config
	fileblockCreator db.FileblockCreator[E]
}

func newInMemoryWal[E db.Entry](c *db.Config, fbc db.FileblockCreator[E]) db.Wal[E] {
	return &inMemoryWal[E]{
		entries:          make(db.Entries[E], 0, c.WalMaxItems),
		cfg:              c,
		fileblockCreator: fbc,
	}
}

func (w *inMemoryWal[E]) Append(d E) (isFull bool) {
	w.entries = append(w.entries, d)
	isFull = len(w.entries) == cap(w.entries)

	if isFull {
		builder := db.NewMetadataBuilder[E]().
			WithLevel(0).
			WithEntries(w.entries).
			WithCreatedAt(time.Now())
		w.fileblockCreator.NewFileblock(w.entries, builder)
		w.entries = make(db.Entries[E], 0, w.cfg.WalMaxItems)
	}

	return isFull
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
		builder := db.NewMetadataBuilder[E]().
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
