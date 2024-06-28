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

	fileblocks []streedb.Fileblock[T]
}

func NewLevel[T streedb.Entry](data []streedb.Fileblock[T]) streedb.Level[T] {
	// find min and max
	meta := data[0].Metadata()
	min := meta.Min
	max := meta.Max
	for _, block := range data {
		meta = block.Metadata()
		if meta.Min.LessThan(min) {
			min = meta.Min
		}
		if max.LessThan(meta.Max) {
			max = meta.Max
		}
	}

	return &Level[T]{fileblocks: data, min: min, max: max}
}

func (l *Level[T]) AppendFile(b streedb.Fileblock[T]) {
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

func (l *Level[T]) RemoveFiles(r map[int]struct{}) {
	if len(r) == 0 {
		return
	}

	temp := make([]streedb.Fileblock[T], 0, len(l.fileblocks)-len(r))

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
	for _, block := range l.fileblocks {
		if v, found, err := block.Find(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
		}
	}

	return nil, false, nil
}

func (l *Level[T]) Close() {
	//noop
}

func hasOverlap[T streedb.Entry](a, b *streedb.MetaFile[T]) bool {
	return b.Min.LessThan(a.Max) && a.Min.LessThan(b.Max)
}

// func hasOverlap[T streedb.Entry](a, b streedb.Metadata[T]) bool {
// 	return b.GetMin().LessThan(a.GetMax()) && a.GetMin().LessThan(b.GetMax())
// }

// func isSizeExceeded[T streedb.Entry](b streedb.Metadata[T], level int) bool {
// 	return b.GetSize() > MAX_LEVEL_0_BLOCK_SIZE*int64(level+1)
// }

func isSizeExceeded[T streedb.Entry](b *streedb.MetaFile[T], level int) bool {
	return b.Size > MAX_LEVEL_0_BLOCK_SIZE*int64(level+1)
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
