package streedb

import "cmp"

type Wal[O cmp.Ordered] interface {
	Append(d Entry[O]) error
	Find(pIdx string, sIdx string, min, max O) (EntryIterator[O], bool)
	Close() error
}

type WalFlushStrategy[O cmp.Ordered] interface {
	ShouldFlush(es *EntriesMap[O]) bool
}
