package streedb

type Wal[E Entry] interface {
	Append(d E) (isFull bool)
	Find(d E) (E, bool)
	Close() error
}
