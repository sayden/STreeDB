package streedb

import (
	"cmp"

	"github.com/thehivecorporation/log"
)

type EntryIterator[O cmp.Ordered, T Entry[O]] interface {
	Next() (Entry[O], bool, error)
}

func newWrapperIterator[O cmp.Ordered, E Entry[O]](btree *BtreeWrapper[O], min, max O) *btreeWrapperIterator[O, E] {
	tree := &btreeWrapperIterator[O, E]{btree: btree}
	tree.Start(max, min)
	return tree
}

type btreeWrapperIterator[O cmp.Ordered, E Entry[O]] struct {
	ch         chan Entry[O]
	btree      *BtreeWrapper[O]
	list       *LinkedList[O, *Fileblock[O, E]]
	item       *Fileblock[O, E]
	entriesMap EntriesMap[O, E]
	index      int
}

func (b *btreeWrapperIterator[O, E]) Start(max, min O) {
	go func() {
		b.btree.BTreeG.AscendRange(&btreeItem[O]{key: min}, &btreeItem[O]{key: max}, func(ll *btreeItem[O]) bool {
			head := ll.val.head
			if head == nil {
				close(b.ch)
				return false
			}

			for next := head; next != nil; next = next.next {
				entriesMap, err := next.value.Load()
				if err != nil {
					close(b.ch)
					return false
				}
				for _, entry := range entriesMap {
					b.ch <- entry
				}
			}

			return true
		})

		close(b.ch)

		log.Debug("btreeWrapperIterator finished")
	}()
}

func (b *btreeWrapperIterator[O, E]) Next() (Entry[O], bool, error) {
	entry := <-b.ch
	if entry == nil {
		return *new(E), false, nil
	}

	return entry, true, nil
}

// func NewIterator[O cmp.Ordered, E Entry[O]](btree BtreeWrapper[O], min, max O) (EntryIterator[O, E], error) {
// iter := newWrapperIterator(&btree, min, max)
// entry, found, err := iter.Next()
// if err != nil {
// 	return nil, err
// }
// if !found {
// 	return nil, errors.New("not found")
// }

// return nil, errors.New("not implemented")
// }

//
// func NewForwardIterator[T Entry](list *MapDLL[T, *Fileblock[T]], k T) (EntryIterator[T], bool) {
// 	var (
// 		entries Entries[T]
// 		index   int
// 		node    *kvDLLNode[T, *Fileblock[T]]
// 		last    *kvDLLNode[T, *Fileblock[T]]
// 		err     error
// 		exit    bool
// 		found   bool
// 	)
//
// 	// FIXME: Probably this doesn't work
// searchLoop:
// 	for node, found = list.Tail(); node != nil && found; node, last = node.prev, node {
// 		for _, rowGroup := range node.value.Rows {
// 			if EntryFallsInsideMinMax(rowGroup.Min, rowGroup.Max, k) {
// 				exit = true
// 				continue searchLoop
// 			}
// 		}
// 		if exit {
// 			break
// 		}
// 	}
// 	if !found || last == nil {
// 		return nil, false
// 	}
//
// 	if entries, err = last.value.Load(); err != nil {
// 		return nil, false
// 	}
//
// 	return &entryForwardIterator[T]{
// 			searchEntry: k,
// 			entries:     entries,
// 			list:        last,
// 			index:       index,
// 		},
// 		true
// }
//
// type entryForwardIterator[T Entry] struct {
// 	searchEntry T
// 	list        *kvDLLNode[T, *Fileblock[T]]
// 	entries     Entries[T]
// 	index       int
// }
//
// func (e *entryForwardIterator[T]) Next() (Entry, bool, error) {
// 	var err error
//
// 	if e.entries == nil {
// 		if e.list == nil {
// 			return nil, false, nil
// 		}
//
// 		e.list = e.list.next
// 		if e.list == nil {
// 			return nil, false, nil
// 		}
//
// 		if e.entries, err = e.list.value.Load(); err != nil {
// 			return nil, false, err
// 		}
// 	}
//
// 	var current Entry
// 	for {
// 		if e.index >= e.entries.Len() {
// 			e.entries = nil
// 			e.index = 0
// 			return e.Next()
// 		}
//
// 		current = e.entries.Get(e.index)
// 		e.index++
// 		if e.searchEntry.LessThan(current) || e.searchEntry.Equals(current) {
// 			break
// 		}
// 	}
//
// 	return current, true, nil
// }
//
// func NewRangeIterator[T Entry](list *MapDLL[T, *Fileblock[T]], min, max T) (EntryIterator[T], bool) {
// 	var (
// 		entries Entries[T]
// 		index   int
// 		found   bool
// 		node    *kvDLLNode[T, *Fileblock[T]]
// 		err     error
// 		exit    bool
// 		last    *kvDLLNode[T, *Fileblock[T]]
// 	)
//
// 	// FIXME: Probably this doesn't work
// searchLoop:
// 	for node, found = list.Tail(); node != nil && found; node, last = node.prev, node {
// 		for _, rowGroup := range node.value.Rows {
// 			if EntryFallsInsideMinMax(rowGroup.Min, rowGroup.Max, min) {
// 				exit = true
// 				continue searchLoop
// 			}
// 		}
// 		if exit {
// 			break
// 		}
// 	}
// 	if !found || last == nil {
// 		return nil, false
// 	}
//
// 	if entries, err = last.value.Load(); err != nil {
// 		return nil, false
// 	}
//
// 	return &entryRangeIterator[T]{
// 			min:     min,
// 			max:     max,
// 			list:    last,
// 			entries: entries,
// 			index:   index,
// 		},
// 		found
// }
//
// type entryRangeIterator[T Entry] struct {
// 	min          T
// 	max          T
// 	list         *kvDLLNode[T, *Fileblock[T]]
// 	entries      Entries[T]
// 	currentBlock *Fileblock[T]
// 	index        int
// 	finished     bool
// }
//
// func (e *entryRangeIterator[T]) Next() (Entry, bool, error) {
// 	if e.finished {
// 		return nil, false, nil
// 	}
//
// 	var err error
//
// 	if e.entries == nil {
// 		if e.list == nil {
// 			return nil, false, nil
// 		}
//
// 		e.list = e.list.next
// 		if e.list == nil {
// 			return nil, false, nil
// 		}
//
// 		if e.entries, err = e.list.value.Load(); err != nil {
// 			return nil, false, err
// 		}
// 	}
//
// 	var val Entry
// 	for {
// 		if e.index >= e.entries.Len() {
// 			e.entries = nil
// 			e.currentBlock = nil
// 			e.index = 0
// 			return e.Next()
// 		}
//
// 		val = e.entries.Get(e.index)
// 		e.index++
// 		if e.max.Equals(val) {
// 			e.finished = true
// 			break
// 		}
//
// 		if e.max.LessThan(val) || e.max.Equals(val) {
// 			return nil, false, nil
// 		}
//
// 		if e.min.LessThan(val) || e.min.Equals(val) {
// 			break
// 		}
//
// 	}
//
// 	return val, true, nil
// }
