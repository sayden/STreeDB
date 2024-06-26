package main

import (
	"time"

	"github.com/google/uuid"
)

type Metadata[T Entry] interface {
	Find(v Entry) (Entry, bool, error)
	Close() error
	Merge(a Metadata[T]) (Metadata[T], error)
	Remove() error

	GetItemCount() int
	GetCreatedAt() time.Time
	GetID() string
	GetMin() Entry
	GetMax() Entry
	GetSize() int64
	GetLevel() int
	GetEntries() (Entries[T], error)
}

func newUUID() string {
	return uuid.New().String()
}
