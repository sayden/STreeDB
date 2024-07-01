package fs

import (
	"errors"
	"fmt"
	"sort"

	"github.com/sayden/streedb"
)

func Merge[T streedb.Entry](a streedb.Fileblock[T], b streedb.Fileblock[T]) (streedb.Entries[T], error) {
	entries, err := a.Load()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to load block '%s'", a.Metadata().DataFilepath), err)
	}

	entries2, err := b.Load()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to load block '%s'", b.Metadata().DataFilepath), err)
	}

	dest := make(streedb.Entries[T], 0, entries.Len()+entries2.Len())
	dest = append(dest, entries...)
	dest = append(dest, entries2...)

	sort.Sort(dest)

	return dest, nil
}
