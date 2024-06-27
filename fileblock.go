package streedb

import (
	"github.com/google/uuid"
)

type DataOps[T Entry] interface {
	Find(v Entry) (Entry, bool, error)
	Close() error
	Merge(a Fileblock[T]) (Fileblock[T], error)
	Remove() error
	GetEntries() (Entries[T], error)
}

// Fileblock represents a block of data that is written to disk.
// A block of data is a list of Entries
// Implementations of Fileblock should be able to read and write data into their respective
// storage formats and retrieve metadata associated with their contents like Min or Max. They
// must not know about the destination of the data files and work mostly with io.Readers and
// io.Writers
type Fileblock[T Entry] interface {
	DataOps[T]
	Metadata[T]
}

func NewUUID() string {
	return uuid.New().String()
}
