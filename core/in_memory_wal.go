package core

import (
	"sort"

	"github.com/sayden/streedb"
	"github.com/sayden/streedb/fileformat"
)

type inMemoryWal[T streedb.Entry] struct {
	data     streedb.Entries[T]
	capacity int
}

func newInMemoryWal[T streedb.Entry](c int) Wal[T] {
	return &inMemoryWal[T]{data: make(streedb.Entries[T], 0, c), capacity: c}
}

func (w *inMemoryWal[T]) Append(d T) (isFull bool) {
	w.data = append(w.data, d)
	isFull = len(w.data) == cap(w.data)
	return
}

func (w *inMemoryWal[T]) Find(d streedb.Entry) (streedb.Entry, bool) {
	for _, v := range w.data {
		if v.Equals(d) {
			return v, true
		}
	}

	return nil, false
}

func (w *inMemoryWal[T]) WriteBlock() (streedb.Metadata[T], error) {
	sort.Sort(w.data)

	block, err := fileformat.NewFileFormat(w.data, 0)
	if err != nil {
		return nil, err
	}
	w.data = make(streedb.Entries[T], 0, w.capacity)

	return block, nil
}

func (w *inMemoryWal[T]) Close() (streedb.Metadata[T], error) {
	if w.data.Len() == 0 {
		return nil, nil
	}

	return w.WriteBlock()
}

func (w *inMemoryWal[T]) GetData() streedb.Entries[T] {
	return w.data
}
