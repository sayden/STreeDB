package streedb

import (
	"cmp"
	"errors"
	"fmt"
)

type FileblockCreator[O cmp.Ordered] interface {
	NewFileblock(es EntriesMap[O], builder *MetadataBuilder[O]) error
}

type FileblockListener[O cmp.Ordered] interface {
	OnFileblockCreated(*Fileblock[O])
	OnFileblockRemoved(*Fileblock[O])
}

func NewFileblock[O cmp.Ordered](cfg *Config, meta *MetaFile[O], filesystem Filesystem[O]) *Fileblock[O] {
	return &Fileblock[O]{
		MetaFile:   *meta,
		cfg:        cfg,
		filesystem: filesystem,
	}
}

type Fileblock[O cmp.Ordered] struct {
	MetaFile[O]

	cfg        *Config
	filesystem Filesystem[O]
}

func (l *Fileblock[O]) Load() (EntriesMap[O], error) {
	return l.filesystem.Load(l)
}

func (l *Fileblock[O]) Find(v Entry[O]) bool {
	for _, rowGroup := range l.Rows {
		if EntryFallsInsideMinMax(rowGroup.Min, rowGroup.Max, v.Min()) {
			return true
		}
	}

	return false
}

func (l *Fileblock[O]) Metadata() *MetaFile[O] {
	return &l.MetaFile
}

func (l *Fileblock[O]) Close() error {
	return nil
}

func (l *Fileblock[O]) UUID() string {
	return l.Uuid
}

func (l *Fileblock[O]) PrimaryIndex() string {
	return l.PrimaryIdx
}

func (l *Fileblock[O]) SecondaryIndex() string {
	return ""
}

func (l *Fileblock[O]) Equals(other Comparable[O]) bool {
	return l.Uuid == other.UUID()
}

func (l *Fileblock[O]) LessThan(other Comparable[O]) bool {
	if l.Min == nil {
		return false
	}

	f, ok := other.(*Fileblock[O])
	if !ok {
		return false
	}

	return *l.Min < *f.Min
}

func Merge[O cmp.Ordered](a, b *Fileblock[O]) (*MetadataBuilder[O], EntriesMap[O], error) {
	entries, err := a.Load()
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("failed to load block '%s'", a.Metadata().DataFilepath), err)
	}

	entries2, err := b.Load()
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("failed to load block '%s'", b.Metadata().DataFilepath), err)
	}

	res, err := entries.Merge(entries2)
	if err != nil {
		return nil, nil, errors.Join(errors.New("failed to merge entries"), err)
	}

	higherLevel := a.Metadata().Level
	if b.Metadata().Level > higherLevel {
		higherLevel = b.Metadata().Level
	}

	// Merge metadatas
	builder := NewMetadataBuilder[O](a.cfg).
		WithLevel(higherLevel).
		WithPrimaryIndex(a.PrimaryIdx).
		WithMin(*a.Min).WithMin(*b.Min).
		WithMax(*a.Max).WithMax(*b.Max)

	return builder, res, nil
}
