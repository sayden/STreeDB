package streedb

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type FileblockCreator[O cmp.Ordered] interface {
	NewFileblock(es *EntriesMap[O], builder *MetadataBuilder[O]) error
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

func (l *Fileblock[O]) Load() (*EntriesMap[O], error) {
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
	sIdx := make([]string, 0, len(l.Rows))
	for _, row := range l.Rows {
		sIdx = append(sIdx, row.SecondaryIdx)
	}

	slices.Sort(sIdx)

	return strings.Join(sIdx, ",")
}

func (l *Fileblock[O]) Equals(other Comparable[O]) bool {
	return l.UUID() == other.UUID()
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

func Merge[O cmp.Ordered](a *Fileblock[O], b ...*Fileblock[O]) (*MetadataBuilder[O], *EntriesMap[O], error) {
	entries, err := a.Load()
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("failed to load block '%s'", a.Metadata().DataFilepath), err)
	}

	builder := NewMetadataBuilder[O](a.cfg)
	var res *EntriesMap[O]
	for _, c := range b {
		entries2, err := c.Load()
		if err != nil {
			return nil, nil, errors.Join(fmt.Errorf("failed to load block '%s'", c.Metadata().DataFilepath), err)
		}

		res, err = entries.Merge(entries2)
		if err != nil {
			return nil, nil, errors.Join(errors.New("failed to merge entries"), err)
		}

		higherLevel := a.Metadata().Level
		if c.Metadata().Level > higherLevel {
			higherLevel = c.Metadata().Level
		}

		builder.WithLevel(higherLevel).
			WithPrimaryIndex(a.PrimaryIdx).
			WithMin(*a.Min).WithMin(*c.Min).
			WithMax(*a.Max).WithMax(*c.Max)
	}

	return builder, res, nil
}
