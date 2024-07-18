package streedb

import "cmp"

type Wal[O cmp.Ordered, E Entry[O]] interface {
	Append(d Entry[O]) error
	Find(pIdx string, sIdx string, min, max O) (E, bool)
	Close() error
}

type WalFlushStrategy[O cmp.Ordered] interface {
	ShouldFlush(es EntriesMap[O]) bool
}
