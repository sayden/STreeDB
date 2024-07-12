package streedb

// type EntryIterator[T Entry] interface {
// 	Next() (Entry, bool, error)
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
