package core

import (
	"errors"
	"fmt"
	"time"

	"github.com/sayden/streedb"
)

type Blocks[T streedb.Entry] []*streedb.MetaFile[T]

type Level[T streedb.Entry] struct {
	min streedb.Entry
	max streedb.Entry

	fileblocks []streedb.Metadata[T]
}

func NewLevel[T streedb.Entry](data []streedb.Metadata[T]) streedb.Level[T] {
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

	return &Level[T]{fileblocks: data, min: min, max: max}
}

func (l *Level[T]) AppendFile(b streedb.Fileblock[T]) {
	// when appending a block, we need to update the min and max
	if b.GetMin().LessThan(l.min) {
		l.min = b.GetMin()
	}
	if l.max.LessThan(b.GetMax()) {
		l.max = b.GetMax()
	}

	l.fileblocks = append(l.fileblocks, b)
}

func (l *Level[T]) RemoveFiles(r map[int]struct{}) {
	if len(r) == 0 {
		return
	}

	temp := make([]streedb.Metadata[T], 0, len(l.fileblocks)-len(r))

	for i := 0; i < len(l.fileblocks); i++ {
		if _, ok := r[i]; ok {
			continue
		}
		temp = append(temp, (l.fileblocks)[i])
	}

	l.fileblocks = temp
}

func (l *Level[T]) Find(d T) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInsideMinMax(l.min, l.max, d) {
		return nil, false, nil
	}

	// iterate through each block
	for _, blockTmp := range l.fileblocks {
		// TODO: This type assertion is indicating some coupling
		block := blockTmp.(streedb.Fileblock[T])
		if v, found, err := block.Find(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
		}
	}

	return nil, false, nil
}

func (l *Level[T]) Close() {
	for _, blockTmp := range l.fileblocks {
		block := blockTmp.(streedb.Fileblock[T])
		block.Close()
	}
}

func hasOverlap[T streedb.Entry](a, b streedb.Metadata[T]) bool {
	return b.GetMin().LessThan(a.GetMax()) && a.GetMin().LessThan(b.GetMax())
}

func isSizeExceeded[T streedb.Entry](b streedb.Metadata[T], level int) bool {
	return b.GetSize() > MAX_LEVEL_0_BLOCK_SIZE*int64(level+1)
}

func isTooOld[T streedb.Entry](b streedb.MetaFile[T], level int) bool {
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
