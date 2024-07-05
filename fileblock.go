package streedb

import (
	"errors"
	"fmt"
	"sort"
)

func NewFileblock[T Entry](cfg *Config, meta *MetaFile[T], filesystem Filesystem[T]) Fileblock[T] {
	return &fileblock[T]{
		MetaFile:   *meta,
		cfg:        cfg,
		filesystem: filesystem,
	}
}

type fileblock[T Entry] struct {
	MetaFile[T]

	cfg        *Config
	filesystem Filesystem[T]
}

func (l *fileblock[T]) Load() (Entries[T], error) {
	return l.filesystem.Load(l)
}

func (l *fileblock[T]) Find(v Entry) bool {
	return EntryFallsInsideMinMax(l.Min, l.Max, v)
}

func (l *fileblock[T]) Metadata() *MetaFile[T] {
	return &l.MetaFile
}

func (l *fileblock[T]) Close() error {
	return nil
}

func (l *fileblock[T]) UUID() string {
	return l.Uuid
}

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
