package streedb

import (
	"errors"
	"fmt"
)

type BasicLevel[T Entry] struct {
	min Entry
	max Entry

	fileblocks []Fileblock[T]
}

func (b *BasicLevel[T]) AppendFile(f Fileblock[T]) {
	// when appending a block, we need to update the min and max
	meta := f.Metadata()
	if meta.Min.LessThan(b.min) {
		b.min = meta.Min
	}
	if b.max.LessThan(meta.Max) {
		b.max = meta.Max
	}

	b.fileblocks = append(b.fileblocks, f)
}

func (b *BasicLevel[T]) RemoveFiles(r map[int]struct{}) {
	if len(r) == 0 {
		return
	}

	temp := make([]Fileblock[T], 0, len(b.fileblocks)-len(r))

	for i := 0; i < len(b.fileblocks); i++ {
		if _, ok := r[i]; ok {
			continue
		}
		temp = append(temp, (b.fileblocks)[i])
	}

	b.fileblocks = temp
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
	//noop
	return nil
}

func (b *BasicLevel[T]) Fileblocks() []Fileblock[T] {
	return b.fileblocks
}

func NewBasicLevels[T Entry](c *Config, fs Filesystem[T]) Levels[T] {
	l := &BasicLevels[T]{
		levels: make(map[int][]Fileblock[T]),
		fs:     fs,
	}

	for i := 0; i < c.MaxLevels+1; i++ {
		l.levels[i] = make([]Fileblock[T], 0)
	}

	return l
}

// BasicLevels is a simple implementation of a list of Levels
type BasicLevels[T Entry] struct {
	levels map[int][]Fileblock[T]
	fs     Filesystem[T]
}

func (b *BasicLevels[T]) GetLevel(i int) []Fileblock[T] {
	return b.levels[i]
}

func (b *BasicLevels[T]) AppendFile(f Fileblock[T]) {
	level := f.Metadata().Level
	b.levels[level] = append(b.levels[level], f)
}

func (b BasicLevels[T]) RemoveFile(a Fileblock[T]) error {
	idx := 0

	meta := a.Metadata()
	level := meta.Level
	// search for block
	for i, block := range b.levels[level] {
		if block.Metadata().Uuid == meta.Uuid {
			// remove block
			if err := b.fs.Remove(block.Metadata()); err != nil {
				return err
			}
			idx = i
			break
		}
	}

	// remove block from slice
	b.levels[level] = append(b.levels[level][:idx], b.levels[level][idx+1:]...)

	return nil
}
func (b BasicLevels[T]) AppendLevel(l Level[T], level int) {
	b.levels[level] = append(b.levels[level], l.Fileblocks()...)
}
