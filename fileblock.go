package streedb

import (
	"cmp"
	"errors"
	"fmt"
)

type FileblockCreator[O cmp.Ordered, E Entry[O]] interface {
	NewFileblock(es Entries[O, E], builder *MetadataBuilder[O]) error
}

type FileblockListener[O cmp.Ordered, E Entry[O]] interface {
	OnFileblockCreated(*Fileblock[O, E])
	OnFileblockRemoved(*Fileblock[O, E])
}

func NewFileblock[O cmp.Ordered, E Entry[O]](cfg *Config, meta *MetaFile[O], filesystem Filesystem[O, E]) *Fileblock[O, E] {
	return &Fileblock[O, E]{
		MetaFile:   *meta,
		cfg:        cfg,
		filesystem: filesystem,
	}
}

type Fileblock[O cmp.Ordered, E Entry[O]] struct {
	MetaFile[O]

	cfg        *Config
	filesystem Filesystem[O, E]
}

func (l *Fileblock[O, E]) Load() (Entries[O, E], error) {
	return l.filesystem.Load(l)
}

func (l *Fileblock[O, E]) Find(v Entry[O]) bool {
	for _, rowGroup := range l.Rows {
		if EntryFallsInsideMinMax(rowGroup.Min, rowGroup.Max, v.Min()) {
			return true
		}
	}

	return false
}

func (l *Fileblock[O, E]) Metadata() *MetaFile[O] {
	return &l.MetaFile
}

func (l *Fileblock[O, E]) Close() error {
	return nil
}

func (l *Fileblock[O, E]) UUID() string {
	return l.Uuid
}

func Merge[O cmp.Ordered, E Entry[O]](a, b *Fileblock[O, E]) (Entries[O, E], error) {
	entries, err := a.Load()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to load block '%s'", a.Metadata().DataFilepath), err)
	}

	entries2, err := b.Load()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to load block '%s'", b.Metadata().DataFilepath), err)
	}

	return entries.Merge(entries2)
}
