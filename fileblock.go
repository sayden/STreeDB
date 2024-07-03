package streedb

import (
	"errors"
	"fmt"
	"sort"
)

type Fileblock[T Entry] interface {
	Close() error
	Find(v Entry) bool
	Load() (Entries[T], error)
	Metadata() *MetaFile[T]
	UUID() string
}

func Merge[T Entry](a Fileblock[T], b Fileblock[T]) (Entries[T], error) {
	entries, err := a.Load()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to load block '%s'", a.Metadata().DataFilepath), err)
	}

	entries2, err := b.Load()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to load block '%s'", b.Metadata().DataFilepath), err)
	}

	dest := make(Entries[T], 0, entries.Len()+entries2.Len())
	dest = append(dest, entries...)
	dest = append(dest, entries2...)

	sort.Sort(dest)

	return dest, nil
}
