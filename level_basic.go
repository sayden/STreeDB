package streedb

import (
	"errors"
	"fmt"
)

func NewBasicLevel[T Entry](cfg *Config, fs Filesystem[T]) Level[T] {
	return &BasicLevel[T]{
		cfg:        cfg,
		fs:         fs,
		fileblocks: make([]Fileblock[T], 0, 10),
	}
}

type BasicLevel[T Entry] struct {
	min        Entry
	max        Entry
	fs         Filesystem[T]
	cfg        *Config
	fileblocks []Fileblock[T]
}

func (b *BasicLevel[T]) AppendFile(f Fileblock[T]) {
	// when appending a block, we need to update the min and max
	meta := f.Metadata()
	if b.min == nil {
		b.min = meta.Min
		b.max = meta.Max
	}

	if meta.Min.LessThan(b.min) {
		b.min = meta.Min
	}
	if b.max.LessThan(meta.Max) {
		b.max = meta.Max
	}

	f.SetFilesystem(b.fs)

	b.fileblocks = append(b.fileblocks, f)
}

func (b *BasicLevel[T]) RemoveFile(f Fileblock[T]) error {
	idx := 0

	meta := f.Metadata()
	// search for block
	for i, block := range b.fileblocks {
		if block.Metadata().Uuid == meta.Uuid {
			// remove block
			if err := b.fs.Remove(block); err != nil {
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

func (b *BasicLevel[T]) Find(d T) (Entry, bool, error) {
	if !EntryFallsInsideMinMax(b.min, b.max, d) {
		return nil, false, nil
	}

	// iterate through each block
	for _, block := range b.fileblocks {
		if v, found, err := block.Find(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
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

func (b *BasicLevel[T]) Fileblocks() []Fileblock[T] {
	return b.fileblocks
}

func NewBasicLevels[T Entry](c *Config, fs Filesystem[T]) Levels[T] {
	l := &BasicLevels[T]{
		levels: make(map[int]Level[T]),
		fs:     fs,
	}

	for i := 0; i < c.MaxLevels+1; i++ {
		l.levels[i] = NewBasicLevel(c, fs)
	}

	return l
}

// BasicLevels is a simple implementation of a list of Levels
type BasicLevels[T Entry] struct {
	levels map[int]Level[T]
	fs     Filesystem[T]
}

func (b *BasicLevels[T]) GetLevel(i int) Level[T] {
	return b.levels[i]
}

func (b *BasicLevels[T]) AppendFile(f Fileblock[T]) {
	level := f.Metadata().Level
	b.levels[level].AppendFile(f)
}

func (b BasicLevels[T]) RemoveFile(a Fileblock[T]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].RemoveFile(a)
}

func (b BasicLevels[T]) AppendLevel(l Level[T], level int) {
	for _, block := range l.Fileblocks() {
		b.levels[level].AppendFile(block)
	}
}

func (b BasicLevels[T]) Close() error {
	for _, level := range b.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}

	return nil
}
