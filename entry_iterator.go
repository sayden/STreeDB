package streedb

import (
	"cmp"
)

type EntryIterator[O cmp.Ordered] interface {
	Next() (Entry[O], bool, error)
}

type IteratorFilter[O cmp.Ordered] func(EntriesMap[O]) bool

func newIteratorWithFilters[O cmp.Ordered](data []*Fileblock[O], filters []EntryFilter) *btreeWrapperIterator[O] {
	sFilters := make([]EntryFilter, 0)
	for _, filter := range filters {
		if filter.Kind() == SecondaryIndexFilterKind {
			sFilters = append(sFilters, filter)
		}
	}

	tree := &btreeWrapperIterator[O]{
		ch: make(chan Entry[O]),
	}

	tree.startFilters(data, sFilters)

	return tree
}

type btreeWrapperIterator[O cmp.Ordered] struct {
	ch    chan Entry[O]
	btree *BtreeIndex[O]
}

func (b *btreeWrapperIterator[O]) startFilters(data []*Fileblock[O], filters []EntryFilter) {
	go func() {
		defer close(b.ch)

		for _, e := range data {
			entriesMap, err := e.Load()
			if err != nil {
				return
			}

			for _, entry := range entriesMap {
				valid := true
				for _, filter := range filters {
					if !filter.Filter(entry) {
						valid = false
						break
					}
				}
				if valid {
					b.ch <- entry
				}
			}
		}
	}()
}

func (b *btreeWrapperIterator[O]) Next() (Entry[O], bool, error) {
	entry := <-b.ch
	if entry == nil {
		return nil, false, nil
	}

	return entry, true, nil
}

func NewSingleItemIterator[O cmp.Ordered](data Entry[O]) *singleItemIterator[O] {
	return &singleItemIterator[O]{data: data}
}

type singleItemIterator[O cmp.Ordered] struct {
	data Entry[O]
}

func (l *singleItemIterator[O]) Next() (Entry[O], bool, error) {
	if l.data == nil {
		return nil, false, nil
	}

	data := l.data
	l.data = nil

	return data, true, nil
}
