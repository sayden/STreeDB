package core

import (
	db "github.com/sayden/streedb"
)

type inMemoryWal[T db.Entry] struct {
	entries db.Entries[T]
	cfg     *db.Config
}

func newInMemoryWal[T db.Entry](c *db.Config) db.Wal[T] {
	return &inMemoryWal[T]{
		entries: make(db.Entries[T], 0, c.WalMaxItems),
		cfg:     c,
	}
}

func (w *inMemoryWal[T]) Append(d T) (isFull bool) {
	w.entries = append(w.entries, d)
	return len(w.entries) == cap(w.entries)
}

func (w *inMemoryWal[T]) Find(d db.Entry) (db.Entry, bool) {
	for _, v := range w.entries {
		if v.Equals(d) {
			return v, true
		}
	}

	return nil, false
}

func (w *inMemoryWal[T]) Close() error {
	return nil
}

func (w *inMemoryWal[T]) GetData() db.Entries[T] {
	return w.entries
}
