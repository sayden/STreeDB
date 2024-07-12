package streedb

import "cmp"

type Wal[O cmp.Ordered, E Entry[O]] interface {
	Append(d E) error
	Find(d E) (E, bool)
	Close() error
}

type WalFlushStrategy[O cmp.Ordered, E Entry[O]] interface {
	ShouldFlush(es Entries[O, E]) bool
}
