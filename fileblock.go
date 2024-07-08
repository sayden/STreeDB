package streedb

import (
	"errors"
	"fmt"
	"sort"
)

type FileblockCreator[T Entry] interface {
	NewFileblock(es Entries[T], initialLevel int) error
}

type FileblockListener[T Entry] interface {
	OnNewFileblock(*Fileblock[T])
	OnFileblockRemoved(*Fileblock[T])
}

func NewFileblock[T Entry](cfg *Config, meta *MetaFile[T], filesystem Filesystem[T]) *Fileblock[T] {
	return &Fileblock[T]{
		MetaFile:   *meta,
		cfg:        cfg,
		filesystem: filesystem,
	}
}

type Fileblock[T Entry] struct {
	MetaFile[T]

	cfg        *Config
	filesystem Filesystem[T]
}

func (l *Fileblock[T]) Load() (Entries[T], error) {
	return l.filesystem.Load(l)
}

func (l *Fileblock[T]) Find(v Entry) bool {
	return EntryFallsInsideMinMax(l.Min, l.Max, v)
}

func (l *Fileblock[T]) Metadata() *MetaFile[T] {
	return &l.MetaFile
}

func (l *Fileblock[T]) Close() error {
	return nil
}

func (l *Fileblock[T]) UUID() string {
	return l.Uuid
}

func Merge[T Entry](a, b *Fileblock[T]) (Entries[T], error) {
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
