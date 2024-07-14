package streedb

import "cmp"

type Wal[O cmp.Ordered, E Entry[O]] interface {
	Append(d E) error
	Find(pIdx string, sIdx string, min, max O) (E, bool)
	Close() error
}

type WalFlushStrategy[O cmp.Ordered, E Entry[O]] interface {
	ShouldFlush(es EntriesMap[O, E]) bool
}
