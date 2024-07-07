package streedb

type EntryIterator[T Entry] interface {
	Next() (Entry, bool, error)
}

func NewForwardIterator[T Entry](list *MapLL[T, Fileblock[T]], fileblock Fileblock[T], k T) (EntryIterator[T], bool) {
	var (
		entries Entries[T]
		index   int
		found   bool
		node    *kvNode[T, Fileblock[T]]
		err error
	)

	for node, found = list.Head(); node != nil && found; node = node.next {
		if EntryFallsInsideMinMax(node.value.Metadata().Min, node.value.Metadata().Max, k) {
			break
		}
	}
	if !found || node == nil {
		return nil, false
	}

	if entries, err = node.value.Load(); err != nil {
		return nil, false
	}
	

	return &entryForwardIterator[T]{
			searchEntry: k,
			entries:     entries,
			list:        node,
			index:       index,
		},
		true
}

type entryForwardIterator[T Entry] struct {
	searchEntry T
	list        *kvNode[T, Fileblock[T]]
	entries     Entries[T]
	index       int
}

func (e *entryForwardIterator[T]) Next() (Entry, bool, error) {
	var err error

	if e.entries == nil {
		if e.list == nil {
			return nil, false, nil
		}

		e.list = e.list.next
		if e.list == nil {
			return nil, false, nil
		}

		if e.entries, err = e.list.value.Load(); err != nil {
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

func NewRangeIterator[T Entry](list *MapLL[T, Fileblock[T]], fileblock Fileblock[T], min, max T) (EntryIterator[T], bool) {
	var (
		entries Entries[T]
		index   int
		found   bool
		node    *kvNode[T, Fileblock[T]]
		err error
	)

	for node, found = list.Head(); node != nil && found; node = node.next {
		if EntryFallsInsideMinMax(node.value.Metadata().Min, node.value.Metadata().Max, min) {
			break
		}
	}
	if !found {
		return nil, false
	}

	if entries, err = node.value.Load(); err != nil {
		return nil, false
	}

	return &entryRangeIterator[T]{
			min:     min,
			max:     max,
			list:    node,
			entries: entries,
			index:   index,
		},
		found
}

type entryRangeIterator[T Entry] struct {
	min          T
	max          T
	list         *kvNode[T, Fileblock[T]]
	entries      Entries[T]
	currentBlock Fileblock[T]
	index        int
	finished     bool
}

func (e *entryRangeIterator[T]) Next() (Entry, bool, error) {
	if e.finished {
		return nil, false, nil
	}

	var err error

	if e.entries == nil {
		if e.list == nil {
			return nil, false, nil
		}

		e.list = e.list.next
		if e.list == nil {
			return nil, false, nil
		}

		if e.entries, err = e.list.value.Load(); err != nil {
			return nil, false, err
		}
	}

	var val Entry
	for {
		if e.index >= len(e.entries) {
			e.entries = nil
			e.currentBlock = nil
			e.index = 0
			return e.Next()
		}

		val = e.entries[e.index]
		e.index++
		if e.max.Equals(val) {
			e.finished = true
			break
		}

		if e.max.LessThan(val) || e.max.Equals(val) {
			return nil, false, nil
		}

		if e.min.LessThan(val) || e.min.Equals(val) {
			break
		}

	}

	return val, true, nil
}
