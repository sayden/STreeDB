package streedb

type Wal[E Entry] interface {
	Append(d E) error
	Find(d E) (E, bool)
	Close() error
}

type WalFlushStrategy[E Entry] interface {
	ShouldFlush(es Entries[E]) bool
}
