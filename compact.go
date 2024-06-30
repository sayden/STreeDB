package streedb

import (
	"sort"
	"time"

	"github.com/emirpasic/gods/v2/sets/treeset"
)

var (
	MAX_LEVELS_TOTAL_BLOCKS = [6]int{
		1,
		1024 * 4,
		1024 * 2,
		1024,
		512,
		256,
	}
)

const (
	MAX_LEVEL_0_TOTAL_BLOCKS = 1024 * 8
	MAX_LEVEL_0_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_0_BLOCK_AGE    = 1 * time.Hour

	MAX_LEVEL_1_TOTAL_BLOCKS = 1024 * 4
	MAX_LEVEL_1_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_1_BLOCK_AGE    = 24 * time.Hour

	MAX_LEVEL_2_TOTAL_BLOCKS = 1024 * 2
	MAX_LEVEL_2_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_2_BLOCK_AGE    = 24 * 7 * time.Hour

	MAX_LEVEL_3_TOTAL_BLOCKS = 1024
	MAX_LEVEL_3_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_3_BLOCK_AGE    = 24 * 15 * time.Hour

	MAX_LEVEL_4_TOTAL_BLOCKS = 512
	MAX_LEVEL_4_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_4_BLOCK_AGE    = 24 * 30 * time.Hour
)

type Compactor[T Entry] interface {
	Compact() ([]Fileblock[T], error)
}

type SameLevelCompactor[T Entry] struct {
	fBuilder FileblockBuilder[T]
	level    Level[T]
}

func (s *SameLevelCompactor[T]) Compact() ([]Fileblock[T], error) {
	fileblocks := s.level.Fileblocks()
	if len(fileblocks) < 1 {
		return fileblocks, nil
	}

	var (
		i                  = 0
		j                  = 1
		aEntries, bEntries Entries[T]
		newFileblock       Fileblock[T]
		err                error
		found              bool
		pairs              = treeset.New[int]()
		a                  Fileblock[T]
		b                  Fileblock[T]
	)

	for i < len(fileblocks) {
		a = fileblocks[i]
		j = i + 1

		for j < len(fileblocks) {
			b = fileblocks[j]
			if pairs.Contains(i); found {
				j++
				continue
			}
			if pairs.Contains(j); found {
				j++
				continue
			}

			if HasOverlap(a.Metadata(), b.Metadata()) || isAdjacent(a.Metadata(), b.Metadata()) {
				if aEntries, err = a.Load(); err != nil {
					continue
				}
				if bEntries, err = b.Load(); err != nil {
					continue
				}

				mergedEntries := MergeSort(aEntries, bEntries)
				if newFileblock, err = s.fBuilder(mergedEntries); err != nil {
					continue
				}

				fileblocks = append(fileblocks, newFileblock)
				pairs.Add(i)
				pairs.Add(j)
				i++
				break
			}
			j++
		}
		i++
	}

	// remove block pairs
	result := make([]Fileblock[T], 0, len(fileblocks))
	for i := 0; i < len(fileblocks); i++ {
		if pairs.Contains(i) {
			continue
		}
		result = append(result, fileblocks[i])
	}

	return result, nil
}

func MergeSort[T Entry](a, b Entries[T]) Entries[T] {
	result := make(Entries[T], 0, a.Len()+b.Len())

	result = append(result, a...)
	result = append(result, b...)

	sort.Sort(result)

	return result
}

func isAdjacent[T Entry](a, b *MetaFile[T]) bool {
	return (a.Max.Adjacent(b.Min) || b.Max.Adjacent(a.Min))
}

func HasOverlap[T Entry](a, b *MetaFile[T]) bool {
	return ((b.Min.LessThan(a.Max) || b.Min.Equals(a.Max)) && (a.Min.LessThan(b.Max) || a.Min.Equals(b.Max)))
}

func IsSizeExceeded[T Entry](b *MetaFile[T], level int) bool {
	return b.Size > MAX_LEVEL_0_BLOCK_SIZE*int64(level+1)
}

func isTooOld[T Entry](b MetaFile[T], level int) bool {
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
