package core

import "github.com/sayden/streedb"

type Wal[T streedb.Entry] interface {
	Append(d T) (isFull bool)
	Find(d streedb.Entry) (streedb.Entry, bool)
	Close() (streedb.Fileblock[T], error)
	GetData() streedb.Entries[T]
}
