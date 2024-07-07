package streedb

func NewIndexedForwardIterator[T Entry](list *Index[T, Fileblock[T]], fileblock Fileblock[T], k T) (EntryIterator[T], bool) {
	var (
		index int
		node  *kvListNode[T, Fileblock[T]]
		last  *kvListNode[T, Fileblock[T]]
		exit  bool
		found bool
	)

	startNodes, _ := list.SearchClosest(k)
	if startNodes == nil {
		return nil, false
	}
	startNodeUUID := startNodes[len(startNodes)-1]
	startNode := list.pairs[startNodeUUID]

	for node, found = startNode, true; node != nil && found; node, last = node.prev, node {
		var i int
		var value Fileblock[T]
		for i, value = len(node.values)-1, node.values[i]; i >= 0; i, value = i-1, node.values[i] {
			if EntryFallsInsideMinMax(value.Metadata().Min, value.Metadata().Max, k) {
				exit = true
				continue
			}
			if exit {
				break
			}
		}

	}
	if !found || last == nil {
		return nil, false
	}

	return &entryIndexedForwardIterator[T]{
			searchEntry: k,
			list:        last,
			index:       index,
		},
		true
}

type entryIndexedForwardIterator[T Entry] struct {
	searchEntry  T
	list         *kvListNode[T, Fileblock[T]]
	entries      Entries[T]
	index        int
	fileblockIdx int
}

func (e *entryIndexedForwardIterator[T]) Next() (Entry, bool, error) {
	var err error

	if e.entries == nil {
		if e.list == nil {
			return nil, false, nil
		}

		block, found := e.list.Next()
		if !found {

		}

		e.list = e.list.next
		if e.list == nil {
			return nil, false, nil
		}

		if e.entries, err = block.Load(); err != nil {
			return nil, false, err
		}
	}

	var current Entry
	for {
		if e.index >= len(e.entries) {
			e.entries = nil
			e.index = 0
			return e.Next()
		}

		current = e.entries[e.index]
		e.index++
		if e.searchEntry.LessThan(current) || e.searchEntry.Equals(current) {
			break
		}
	}

	return current, true, nil
}

// func NewRangeIterator[T Entry](list *MapDLL[T, Fileblock[T]], fileblock Fileblock[T], min, max T) (EntryIterator[T], bool) {
// 	var (
// 		entries Entries[T]
// 		index   int
// 		found   bool
// 		node    *kvDLLNode[T, Fileblock[T]]
// 		err     error
// 		exit    bool
// 		last    *kvDLLNode[T, Fileblock[T]]
// 	)
//
// 	for node, found = list.Tail(); node != nil && found; node, last = node.prev, node {
// 		if EntryFallsInsideMinMax(node.value.Metadata().Min, node.value.Metadata().Max, min) {
// 			exit = true
// 			continue
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
// 	list         *kvDLLNode[T, Fileblock[T]]
// 	entries      Entries[T]
// 	currentBlock Fileblock[T]
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
// 		if e.index >= len(e.entries) {
// 			e.entries = nil
// 			e.currentBlock = nil
// 			e.index = 0
// 			return e.Next()
// 		}
//
// 		val = e.entries[e.index]
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
