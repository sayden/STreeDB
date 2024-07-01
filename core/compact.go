package core

import (
	"errors"
	"time"

	"github.com/emirpasic/gods/v2/sets/treeset"
	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

func NewSameLevelCompactor[T streedb.Entry](cfg *streedb.Config, fs streedb.Filesystem[T], level streedb.Level[T]) streedb.Compactor[T] {
	return &SingleLevelCompactor[T]{
		fs:    fs,
		level: level,
		cfg:   cfg,
	}
}

type SingleLevelCompactor[T streedb.Entry] struct {
	fs    streedb.Filesystem[T]
	level streedb.Level[T]
	cfg   *streedb.Config
}

func (s *SingleLevelCompactor[T]) Compact() ([]streedb.Fileblock[T], error) {
	fileblocks := s.level.Fileblocks()
	if len(fileblocks) < 1 {
		return fileblocks, nil
	}
	var (
		passes         = s.cfg.CompactionPasses
		i              = 0
		j              = 1
		err            error
		found          bool
		blocksToRemove = treeset.New[int]()
		a              streedb.Fileblock[T]
		b              streedb.Fileblock[T]
	)

compactionLoop:
	initialLen := len(fileblocks)
	for i < initialLen {
		a = fileblocks[i]
		j = i + 1

		for j < initialLen {
			b = fileblocks[j]
			if blocksToRemove.Contains(i); found {
				j++
				continue
			}
			if blocksToRemove.Contains(j); found {
				j++
				continue
			}

			if HasOverlap(a.Metadata(), b.Metadata()) || isAdjacent(a.Metadata(), b.Metadata()) {
				newFileblock, err := s.fs.Merge(a, b)
				if err != nil {
					log.WithError(err).Error("failed to create new fileblock")
					continue
				}

				fileblocks = append(fileblocks, newFileblock)
				blocksToRemove.Add(i)
				blocksToRemove.Add(j)
				i++
				break
			}
			j++
		}
		i++
	}

	// remove blocks
	result := make([]streedb.Fileblock[T], 0, len(fileblocks))
	for i := 0; i < len(fileblocks); i++ {
		if blocksToRemove.Contains(i) {
			if err = s.fs.Remove(fileblocks[i].Metadata()); err != nil {
				return nil, errors.Join(errors.New("error deleting block during compaction"), err)
			}
			continue
		}
		result = append(result, fileblocks[i])
	}

	return result, nil
}

func NewTieredCompactor[T streedb.Entry](cfg *streedb.Config, fs streedb.Filesystem[T], fBuilder streedb.FileblockBuilder[T], levels streedb.Levels[T]) streedb.MultiLevelCompactor[T] {
	return &TieredCompactor[T]{
		cfg:    cfg,
		levels: levels,
		fs:     fs,
	}
}

type TieredCompactor[T streedb.Entry] struct {
	fs     streedb.Filesystem[T]
	cfg    *streedb.Config
	levels streedb.Levels[T]
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
	for levelIdx := 0; levelIdx < t.cfg.MaxLevels; levelIdx++ {
		level := t.levels.GetLevel(levelIdx)
		mergedFileblocks = append(mergedFileblocks, level...)
	}

	same := NewSameLevelCompactor(t.fs, streedb.NewLevel(mergedFileblocks))
	blocks, err := same.Compact()
	if err != nil {
		return nil, err
	}

	newLevels := streedb.NewLevels[T](t.cfg, t.fs)
	for _, block := range blocks {
		newLevels.AppendFile(block)
	}

	return newLevels, nil
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
