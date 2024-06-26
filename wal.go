package main

import "sort"

type Wal[T Entry] struct {
	data     Entries[T]
	capacity int
}

func NewWal[T Entry](c int) *Wal[T] {
	return &Wal[T]{data: make(Entries[T], 0, c), capacity: c}
}

func (w *Wal[T]) Append(d T) (isFull bool) {
	w.data = append(w.data, d)
	isFull = len(w.data) == cap(w.data)
	return
}

func (w *Wal[T]) Find(d Entry) (Entry, bool) {
	for _, v := range w.data {
		if v.Equals(d) {
			return v, true
		}
	}

	return nil, false
}

func (w *Wal[T]) WriteBlock() (Metadata[T], error) {
	sort.Sort(w.data)

	block, err := NewParquetBlock[T](w.data, 0)
	if err != nil {
		return nil, err
	}
	w.data = make(Entries[T], 0, w.capacity)

	return block, nil
}

func (w *Wal[T]) Close() (Metadata[T], error) {
	if w.data.Len() == 0 {
		return nil, nil
	}

	return w.WriteBlock()
}
