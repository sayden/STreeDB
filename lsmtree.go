package streedb

import "cmp"

type LsmTreeOps[O cmp.Ordered, E Entry[O]] interface {
	Append(d Entry[O]) error
	Find(pIdx, sIdx string, min, max O) (EntryIterator[O], bool, error)
	Close() error
	Compact() error
}
