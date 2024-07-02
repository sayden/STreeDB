package streedb

type Wal[T Entry] interface {
	Append(d T) (isFull bool)
	Find(d Entry) (Entry, bool)
	Close() (Fileblock[T], error)
	GetData() Entries[T]
}
