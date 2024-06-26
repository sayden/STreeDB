package main

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	MAX_LEVELS = 5
)

type Blocks[T Entry] []*Block[T]

type Level[T Entry] struct {
	min  Entry
	max  Entry
	data []Metadata[T]
}

func NewEmptyLevel[T Entry](c int) Level[T] {
	return Level[T]{data: make([]Metadata[T], 0, c)}
}

func NewLevel[T Entry](data []Metadata[T]) Level[T] {
	// find min and max
	min := data[0].GetMin()
	max := data[0].GetMax()
	for _, block := range data {
		if block.GetMin().LessThan(min) {
			min = block.GetMin()
		}
		if max.LessThan(block.GetMax()) {
			max = block.GetMax()
		}
	}

	return Level[T]{data: data, min: min, max: max}
}

func (l *Level[T]) AppendBlock(b Metadata[T]) {
	// when appending a block, we need to update the min and max
	if b.GetMin().LessThan(l.min) {
		l.min = b.GetMin()
	}
	if l.max.LessThan(b.GetMax()) {
		l.max = b.GetMax()
	}

	l.data = append(l.data, b)
}

func (l *Level[T]) RemoveBlocks(r map[int]struct{}) {
	if len(r) == 0 {
		return
	}

	temp := make([]Metadata[T], 0, len(l.data)-len(r))

	for i := 0; i < len(l.data); i++ {
		if _, ok := r[i]; ok {
			continue
		}
		temp = append(temp, (l.data)[i])
	}

	l.data = temp
}

func (l *Level[T]) Find(d T) (Entry, bool, error) {
	if !l.fallsInside(d) {
		return nil, false, nil
	}

	// iterate through each block
	for _, block := range l.data {
		if v, found, err := block.Find(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
		}
	}

	return nil, false, nil
}

func (l *Level[T]) Close() {
	for _, block := range l.data {
		block.Close()
	}
}

func (b *Level[T]) fallsInside(d Entry) bool {
	return b.min.LessThan(d) && d.LessThan(b.max)
}

type Levels[T Entry] map[int][]Metadata[T]

func NewLevels[T Entry]() Levels[T] {
	l := make(map[int][]Metadata[T], MAX_LEVELS+1)

	for i := 0; i < MAX_LEVELS+1; i++ {
		l[i] = make([]Metadata[T], 0)
	}

	return l
}

func (l Levels[T]) AppendBlock(b Metadata[T]) {
	l[b.GetLevel()] = append(l[b.GetLevel()], b)
}

func (l Levels[T]) RemoveBlock(b Metadata[T]) error {
	idx := 0

	// search for block
	for i, block := range l[b.GetLevel()] {
		if block.GetID() == b.GetID() {
			// remove block
			if err := b.Remove(); err != nil {
				return err
			}
			idx = i
			break
		}
	}

	// remove block from slice
	l[b.GetLevel()] = append(l[b.GetLevel()][:idx], l[b.GetLevel()][idx+1:]...)

	return nil
}

// mergeEntries two same-size+overlapping sorted blocks into one sorted block
func mergeEntries[T Entry](a, b Entries[T], level int) (Metadata[T], error) {
	// TODO: implement fs merge too
	newData := make(Entries[T], 0, len(a)+len(b))
	newData = append(newData, a...)
	newData = append(newData, b...)

	sort.Sort(newData)

	return NewParquetBlock(newData, level)
}

func hasOverlap[T Entry](a, b Metadata[T]) bool {
	return b.GetMin().LessThan(a.GetMax()) && a.GetMin().LessThan(b.GetMax())
}

func isSizeExceeded[T Entry](b Metadata[T], level int) bool {
	return b.GetSize() > MAX_LEVEL_0_BLOCK_SIZE*int64(level+1)
}

func isTooOld[T Entry](b Block[T], level int) bool {
	switch level {
	case 0:
		return time.Since(b.CreatedAt) > MAX_LEVEL_0_BLOCK_AGE
	case 1:
		return time.Since(b.CreatedAt) > MAX_LEVEL_1_BLOCK_AGE
	case 2:
		return time.Since(b.CreatedAt) > MAX_LEVEL_2_BLOCK_AGE
	case 3:
		return time.Since(b.CreatedAt) > MAX_LEVEL_3_BLOCK_AGE
	case 4:
		return time.Since(b.CreatedAt) > MAX_LEVEL_4_BLOCK_AGE
	default:
		return false
	}
}
