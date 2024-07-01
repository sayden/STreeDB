package core

import (
	"sort"
	"time"

	"github.com/emirpasic/gods/v2/sets/treeset"
	"github.com/sayden/streedb"
)

type SameLevelCompactor[T streedb.Entry] struct {
	fBuilder streedb.FileblockBuilder[T]
	level    streedb.Level[T]
}

func (s *SameLevelCompactor[T]) Compact() ([]streedb.Fileblock[T], error) {
	fileblocks := s.level.Fileblocks()
	if len(fileblocks) < 1 {
		return fileblocks, nil
	}

	var (
		i                  = 0
		j                  = 1
		aEntries, bEntries streedb.Entries[T]
		newFileblock       streedb.Fileblock[T]
		err                error
		found              bool
		pairs              = treeset.New[int]()
		a                  streedb.Fileblock[T]
		b                  streedb.Fileblock[T]
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
				highestLevel := b.Metadata().Level
				if a.Metadata().Level > highestLevel {
					highestLevel = a.Metadata().Level
				}

				mergedEntries := MergeSort(aEntries, bEntries)
				if newFileblock, err = s.fBuilder(mergedEntries, highestLevel); err != nil {
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
	result := make([]streedb.Fileblock[T], 0, len(fileblocks))
	for i := 0; i < len(fileblocks); i++ {
		if pairs.Contains(i) {
			continue
		}
		result = append(result, fileblocks[i])
	}

	return result, nil
}

func NewTieredCompactor[T streedb.Entry](cfg *streedb.Config, fBuilder streedb.FileblockBuilder[T], levels streedb.Levels[T]) streedb.MultiLevelCompactor[T] {
	return &TieredCompactor[T]{
		fBuilder: fBuilder,
		cfg:      cfg,
		levels:   levels,
	}
}

type TieredCompactor[T streedb.Entry] struct {
	fBuilder streedb.FileblockBuilder[T]
	same     SameLevelCompactor[T]
	cfg      *streedb.Config
	levels   streedb.Levels[T]
}

func (t *TieredCompactor[T]) Compact() (streedb.Levels[T], error) {
	totalFileblocks := 0

	for level := 0; level < t.cfg.MaxLevels; level++ {
		totalFileblocks += len(t.levels.GetLevel(level))
	}
	if totalFileblocks == 0 {
		return nil, nil
	}

	mergedFileblocks := make([]streedb.Fileblock[T], 0, totalFileblocks)
	for level := 0; level < t.cfg.MaxLevels; level++ {
		mergedFileblocks = append(mergedFileblocks, t.levels.GetLevel(level)...)
	}

	same := SameLevelCompactor[T]{
		fBuilder: t.fBuilder,
		level:    streedb.NewLevel(mergedFileblocks),
	}
	blocks, err := same.Compact()
	if err != nil {
		return nil, err
	}

	newLevels := streedb.NewLevels[T](t.cfg)
	for _, block := range blocks {
		newLevels.AppendFile(block)
	}

	return newLevels, nil
}

func MergeSort[T streedb.Entry](a, b streedb.Entries[T]) streedb.Entries[T] {
	result := make(streedb.Entries[T], 0, a.Len()+b.Len())

	result = append(result, a...)
	result = append(result, b...)

	sort.Sort(result)

	return result
}

func isAdjacent[T streedb.Entry](a, b *streedb.MetaFile[T]) bool {
	return (a.Max.Adjacent(b.Min) || b.Max.Adjacent(a.Min))
}

func HasOverlap[T streedb.Entry](a, b *streedb.MetaFile[T]) bool {
	return ((b.Min.LessThan(a.Max) || b.Min.Equals(a.Max)) && (a.Min.LessThan(b.Max) || a.Min.Equals(b.Max)))
}

func IsSizeExceeded[T streedb.Entry](b *streedb.MetaFile[T], level int) bool {
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
