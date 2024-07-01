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

func (l *BasicLevel[T]) AppendFile(b Fileblock[T]) {
	// when appending a block, we need to update the min and max
	meta := b.Metadata()
	if meta.Min.LessThan(l.min) {
		l.min = meta.Min
	}
	if l.max.LessThan(meta.Max) {
		l.max = meta.Max
	}

	l.fileblocks = append(l.fileblocks, b)
}

func (l *BasicLevel[T]) RemoveFiles(r map[int]struct{}) {
	if len(r) == 0 {
		return
	}

	temp := make([]Fileblock[T], 0, len(l.fileblocks)-len(r))

	for i := 0; i < len(l.fileblocks); i++ {
		if _, ok := r[i]; ok {
			continue
		}
		temp = append(temp, (l.fileblocks)[i])
	}

	l.fileblocks = temp
}

func (l *BasicLevel[T]) Find(d T) (Entry, bool, error) {
	if !EntryFallsInsideMinMax(l.min, l.max, d) {
		return nil, false, nil
	}

	// iterate through each block
	for _, block := range l.fileblocks {
		if v, found, err := block.Find(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
		}
	}

	return nil, false, nil
}

func (l *BasicLevel[T]) Close() error {
	//noop
	return nil
}

func (l *BasicLevel[T]) Fileblocks() []Fileblock[T] {
	return l.fileblocks
}

// BasicLevels is a simple implementation of a list of Levels
type BasicLevels[T Entry] map[int][]Fileblock[T]

func (l BasicLevels[T]) GetLevel(i int) []Fileblock[T] {
	return l[i]
}

func (l BasicLevels[T]) AppendFile(b Fileblock[T]) {
	level := b.Metadata().Level
	l[level] = append(l[level], b)
}

func (l BasicLevels[T]) RemoveFile(b Fileblock[T]) error {
	idx := 0

	meta := b.Metadata()
	level := meta.Level
	// search for block
	for i, block := range l[level] {
		if block.Metadata().Uuid == meta.Uuid {
			// remove block
			if err := b.Remove(); err != nil {
				return err
			}
			idx = i
			break
		}
	}

	// remove block from slice
	l[level] = append(l[level][:idx], l[level][idx+1:]...)

	return nil
}
