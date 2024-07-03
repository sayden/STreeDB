package fs

import (
	"errors"
	"fmt"

	db "github.com/sayden/streedb"
)

// NewLevels is redundant atm because there is only one implementation of Levels, but facilitates
// refactor
func NewLevels[T db.Entry](c *db.Config, fs db.Filesystem[T]) db.Levels[T] {
	return NewSingleFsLevels(c, fs)
}

func NewBasicLevel[T db.Entry](cfg *db.Config, fs db.Filesystem[T]) db.Level[T] {
	level := &BasicLevel[T]{
		cfg:        cfg,
		Filesystem: fs,
		fileblocks: make([]db.Fileblock[T], 0, 10),
	}
	fs.OpenMetaFilesInLevel(level)

	return level
}

type BasicLevel[T db.Entry] struct {
	db.Filesystem[T]

	min db.Entry
	max db.Entry
	cfg *db.Config

	// TODO: Use a Btree instead of a slice
	fileblocks []db.Fileblock[T]
}

func (b *BasicLevel[T]) AppendFileblock(f db.Fileblock[T]) error {
	// when appending a block, we need to update the min and max
	meta := f.Metadata()
	if b.min == nil || b.max == nil {
		b.min = meta.Min
		b.max = meta.Max
	}

	if meta.Min.LessThan(b.min) {
		b.min = meta.Min
	}
	if b.max.LessThan(meta.Max) {
		b.max = meta.Max
	}

	b.fileblocks = append(b.fileblocks, f)

	return nil
}

func (b *BasicLevel[T]) Create(es db.Entries[T], meta *db.MetadataBuilder[T]) error {
	// when appending a block, we need to update the min and max
	if b.min == nil || b.max == nil {
		b.min = meta.Min
		b.max = meta.Max
	}

	if meta.Min.LessThan(b.min) {
		b.min = meta.Min
	}
	if b.max.LessThan(meta.Max) {
		b.max = meta.Max
	}

	// Add filesystem related information to the metadata
	metadata, err := b.Filesystem.FillMetadataBuilder(meta).Build()
	if err != nil {
		return err
	}

	fileblock, err := b.Filesystem.Create(b.cfg, es, metadata)
	if err != nil {
		return err
	}

	b.fileblocks = append(b.fileblocks, fileblock)

	return nil
}

func (b *BasicLevel[T]) RemoveFile(f db.Fileblock[T]) error {
	idx := 0

	meta := f.Metadata()
	// search for block
	for i, block := range b.fileblocks {
		if block.Metadata().Uuid == meta.Uuid {
			// remove block
			if err := b.Remove(block); err != nil {
				return err
			}
			idx = i
			break
		}
	}

	// remove block from slice
	b.fileblocks = append(b.fileblocks[:idx], b.fileblocks[idx+1:]...)

	return nil
}

func (b *BasicLevel[T]) Find(d T) (db.Entry, bool, error) {
	if !db.EntryFallsInsideMinMax(b.min, b.max, d) {
		return nil, false, nil
	}

	// iterate through each block
	for _, fileblock := range b.fileblocks {
		if found := fileblock.Find(d); found {
			entries, err := fileblock.Load()
			if err != nil {
				return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
			}

			entry, found := entries.Find(d)
			return entry, found, nil
		}
	}

	return nil, false, nil
}

func (b *BasicLevel[T]) Close() error {
	for _, block := range b.fileblocks {
		block.Close()
	}

	return nil
}

func (b *BasicLevel[T]) Fileblocks() []db.Fileblock[T] {
	return b.fileblocks
}

// Deprecated: Only useful with single filesystems
func NewSingleFsLevels[T db.Entry](c *db.Config, fs db.Filesystem[T]) db.Levels[T] {
	l := &BasicLevels[T]{
		levels: make(map[int]db.Level[T]),
		fs:     fs,
	}

	for i := 0; i < c.MaxLevels+1; i++ {
		l.levels[i] = NewBasicLevel(c, fs)
	}

	return l
}

// BasicLevels is a simple implementation of a list of Levels
type BasicLevels[T db.Entry] struct {
	levels map[int]db.Level[T]
	fs     db.Filesystem[T]
}

func (b *BasicLevels[T]) Create(es db.Entries[T], initialLevel int) error {
	panic("implement me")
}

func (b *BasicLevels[T]) GetLevel(i int) db.Level[T] {
	return b.levels[i]
}

func (b *BasicLevels[T]) RemoveFile(a db.Fileblock[T]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].RemoveFile(a)
}

func (b *BasicLevels[T]) Close() error {
	for _, level := range b.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (b *BasicLevels[T]) AppendFileblock(a db.Fileblock[T]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].AppendFileblock(a)
}
