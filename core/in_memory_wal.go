package core

import (
	"github.com/sayden/streedb"
)

type inMemoryWal[T streedb.Entry] struct {
	entries streedb.Entries[T]
	cfg     *streedb.Config
}

func newInMemoryWal[T streedb.Entry](c *streedb.Config) Wal[T] {
	return &inMemoryWal[T]{entries: make(streedb.Entries[T], 0, c.WalMaxItems), cfg: c}
}

func (w *inMemoryWal[T]) Append(d T) (isFull bool) {
	w.entries = append(w.entries, d)
	isFull = len(w.entries) == cap(w.entries)
	return
}

func (w *inMemoryWal[T]) Find(d streedb.Entry) (streedb.Entry, bool) {
	for _, v := range w.entries {
		if v.Equals(d) {
			return v, true
		}
	}

	return nil, false
}

func (w *inMemoryWal[T]) Close() (streedb.Fileblock[T], error) {
	// return w.WriteBlock()
	return nil, nil
}

func (w *inMemoryWal[T]) GetData() streedb.Entries[T] {
	return w.entries
}
