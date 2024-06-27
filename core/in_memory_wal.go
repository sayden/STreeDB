package core

import (
	"sort"

	"github.com/sayden/streedb"
	"github.com/sayden/streedb/fileformat"
)

type inMemoryWal[T streedb.Entry] struct {
	entries  streedb.Entries[T]
	capacity int
}

func newInMemoryWal[T streedb.Entry](c int) Wal[T] {
	return &inMemoryWal[T]{entries: make(streedb.Entries[T], 0, c), capacity: c}
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

func (w *inMemoryWal[T]) WriteBlock() (streedb.Fileblock[T], error) {
	sort.Sort(w.entries)

	block, err := fileformat.NewFile(w.entries, 0)
	if err != nil {
		return nil, err
	}
	w.entries = make(streedb.Entries[T], 0, w.capacity)

	return block, nil
}

func (w *inMemoryWal[T]) Close() (streedb.Fileblock[T], error) {
	if w.entries.Len() == 0 {
		return nil, nil
	}

	return w.WriteBlock()
}

func (w *inMemoryWal[T]) GetData() streedb.Entries[T] {
	return w.entries
}
