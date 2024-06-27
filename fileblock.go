package streedb

import (
	"github.com/google/uuid"
)

type DataOps[T Entry] interface {
	Find(v Entry) (Entry, bool, error)
	Close() error
	Merge(a Fileblock[T]) (Fileblock[T], error)
	Remove() error
}

type Fileblock[T Entry] interface {
	DataOps[T]
	Metadata[T]
}

func NewUUID() string {
	return uuid.New().String()
}
