package streedb

import "github.com/sayden/streedb/bplustree"

type EntryIterator[T Entry] interface {
	Next() (Entry, bool, error)
}

func NewForwardIterator[T Entry](tree *bplustree.Tree[T, Fileblock[T]], fileblock Fileblock[T], k T) (EntryIterator[T], bool, error) {
	iter, found := tree.Seek(fileblock.Metadata().Min)
	if !found {
		return nil, false, nil
	}

	var (
		err          error
		currentBlock Fileblock[T]
		entries      Entries[T]
		index        int
		entry        Entry
	)

searchLoop:
	for {
		if _, currentBlock, err = iter.Next(); err != nil {
			return nil, false, err
		}

		if entries, err = currentBlock.Load(); err != nil {
			return nil, false, err
		}

		for index, entry = range entries {
			if entry.Equals(k) {
				break searchLoop
			}
		}

		index = 0
	}

	return &entryForwardIterator[T]{
			searchEntry:  k,
			iter:         iter,
			entries:      entries,
			currentBlock: currentBlock,
			index:        index,
		},
		found, nil
}

type entryForwardIterator[T Entry] struct {
	searchEntry  T
	iter         *bplustree.Enumerator[T, Fileblock[T]]
	entries      Entries[T]
	currentBlock Fileblock[T]
	index        int
}

func (e *entryForwardIterator[T]) Next() (Entry, bool, error) {
	var err error

	if e.currentBlock == nil {
		if _, e.currentBlock, err = e.iter.Next(); err != nil {
			return nil, false, err
		}
		if e.entries, err = e.currentBlock.Load(); err != nil {
			return nil, false, err
		}
	}

	var val Entry
	for {
		if e.index >= len(e.entries) {
			e.currentBlock = nil
			e.index = 0
			return e.Next()
		}

		val = e.entries[e.index]
		e.index++
		if e.searchEntry.LessThan(val) || e.searchEntry.Equals(val) {
			break
		}
	}

	return val, true, nil
}

func NewRangeIterator[T Entry](tree *bplustree.Tree[T, Fileblock[T]], fileblock Fileblock[T], min, max T) (EntryIterator[T], bool, error) {
	iter, found := tree.Seek(fileblock.Metadata().Min)
	if !found {
		return nil, false, nil
	}

	var (
		err          error
		currentBlock Fileblock[T]
		entries      Entries[T]
		index        int
		entry        Entry
	)

searchLoop:
	for {
		if _, currentBlock, err = iter.Next(); err != nil {
			return nil, false, err
		}

		if entries, err = currentBlock.Load(); err != nil {
			return nil, false, err
		}

		for index, entry = range entries {
			if entry.Equals(min) {
				break searchLoop
			}
		}

		index = 0
	}

	return &entryRangeIterator[T]{
			min:          min,
			max:          max,
			iter:         iter,
			entries:      entries,
			currentBlock: currentBlock,
			index:        index,
		},
		found, nil
}

type entryRangeIterator[T Entry] struct {
	min          T
	max          T
	iter         *bplustree.Enumerator[T, Fileblock[T]]
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

	if e.currentBlock == nil {
		if _, e.currentBlock, err = e.iter.Next(); err != nil {
			return nil, false, err
		}
		if e.entries, err = e.currentBlock.Load(); err != nil {
			return nil, false, err
		}
	}

	var val Entry
	for {
		if e.index >= len(e.entries) {
			e.currentBlock = nil
			e.index = 0
			return e.Next()
		}

		val = e.entries[e.index]
		e.index++
		if e.min.LessThan(val) || e.min.Equals(val) {
			break
		}

		if e.max.Equals(val) {
			e.finished = true
			break
		}

		if e.max.LessThan(val) || e.max.Equals(val) {
			return nil, false, nil
		}
	}

	return val, true, nil
}
