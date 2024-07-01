package core

import (
	"errors"
	"math"
	"time"

	"github.com/emirpasic/gods/v2/sets/treeset"
	"github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
)

func NewSingleLevelCompactor[T streedb.Entry](cfg *streedb.Config, fs streedb.Filesystem[T], level streedb.Level[T]) streedb.Compactor[T] {
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
		blocksToRemove = treeset.New[string]()
		blocksToSkip   = treeset.New[string]()
	)

	var (
		i            = 0
		j            = 1
		err          error
		a            streedb.Fileblock[T]
		b            streedb.Fileblock[T]
		newFileblock streedb.Fileblock[T]
		entries      streedb.Entries[T]
	)

	initialLen := len(fileblocks)
	for i < initialLen {
		a = fileblocks[i]
		if blocksToSkip.Contains(a.UUID()) || blocksToRemove.Contains(a.UUID()) {
			i++
			continue
		}
		j = i + 1

		for j < initialLen {
			b = fileblocks[j]

			// don't try to merge level 5 with level 1 blocks to reduce write amplification
			areNonAdjacentLevels := math.Abs(float64(a.Metadata().Level-b.Metadata().Level)) > 1

			if blocksToSkip.Contains(b.UUID()) || blocksToRemove.Contains(b.UUID()) || areNonAdjacentLevels {
				j++
				continue
			}

			if HasOverlap(a.Metadata(), b.Metadata()) || isAdjacent(a.Metadata(), b.Metadata()) {
				if entries, err = fs.Merge(a, b); err != nil {
					return nil, errors.Join(errors.New("failed to create new fileblock"), err)
				}

				// Write the new block to storage directly
				if newFileblock, err = s.fs.Create(s.cfg, entries, a.Metadata().Level); err != nil {
					return nil, errors.Join(errors.New("failed to create new fileblock"), err)
				}

				fileblocks = append(fileblocks, newFileblock)
				blocksToSkip.Add(newFileblock.UUID())

				blocksToRemove.Add(a.UUID())
				blocksToRemove.Add(b.UUID())

				// current i,j pair have been merged, so we can skip the next i and trust
				// blocksToRemove to skip j in a future iteration
				i++

				break
			}
			j++
		}
		i++
	}

	// Remove flagged blocks
	result := make([]streedb.Fileblock[T], 0, len(fileblocks))
	for i := 0; i < len(fileblocks); i++ {
		block := fileblocks[i]

		if blocksToRemove.Contains(block.UUID()) {
			if err = s.fs.Remove(block); err != nil {
				return nil, errors.Join(errors.New("error deleting block during compaction"), err)
			}
			continue
		}

		// Untouched fileblock
		result = append(result, block)
	}

	return result, nil
}

func NewTieredCompactor[T streedb.Entry](
	cfg *streedb.Config,
	fs streedb.Filesystem[T],
	fBuilder streedb.FileblockBuilder[T],
	levels streedb.Levels[T],
	promoter streedb.LevelPromoter[T]) streedb.MultiLevelCompactor[T] {
	return &TieredCompactor[T]{
		cfg:      cfg,
		levels:   levels,
		fs:       fs,
		promoter: promoter,
	}
}

type TieredCompactor[T streedb.Entry] struct {
	fs       streedb.Filesystem[T]
	cfg      *streedb.Config
	levels   streedb.Levels[T]
	promoter streedb.LevelPromoter[T]
}

func (t *TieredCompactor[T]) Compact() (streedb.Levels[T], error) {
	blocks, err := t.compact()
	if err != nil {
		return nil, err
	}

	blocks, err = t.promoter.Promote(blocks)
	if err != nil {
		return nil, err
	}

	newLevels := streedb.NewLevels(t.cfg, t.fs)
	for _, block := range blocks {
		newLevels.AppendFile(block)
	}

	return newLevels, nil
}

func (t *TieredCompactor[T]) compact() ([]streedb.Fileblock[T], error) {
	totalFileblocks := 0

	for level := 0; level < t.cfg.MaxLevels; level++ {
		totalFileblocks += len(t.levels.GetLevel(level).Fileblocks())
	}

	if totalFileblocks == 0 {
		return nil, nil
	}

	mergedFileblocks := make([]streedb.Fileblock[T], 0, totalFileblocks)
	for levelIdx := 0; levelIdx < t.cfg.MaxLevels; levelIdx++ {
		level := t.levels.GetLevel(levelIdx)
		mergedFileblocks = append(mergedFileblocks, level.Fileblocks()...)
	}

	same := NewSingleLevelCompactor(t.cfg, t.fs, streedb.NewLevel(mergedFileblocks))
	blocks, err := same.Compact()
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

func NewItemLimitPromoter[T streedb.Entry](cfg *streedb.Config, fs streedb.Filesystem[T], maxItems int) streedb.LevelPromoter[T] {
	return &ItemLimitPromoter[T]{
		fs:       fs,
		cfg:      cfg,
		maxItems: maxItems,
	}
}

type ItemLimitPromoter[T streedb.Entry] struct {
	fs       streedb.Filesystem[T]
	cfg      *streedb.Config
	maxItems int
}

func (i *ItemLimitPromoter[T]) Promote(blocks []streedb.Fileblock[T]) ([]streedb.Fileblock[T], error) {
	if len(blocks) == 0 {
		return nil, nil
	}

	for _, block := range blocks {
		realLevel := block.Metadata().ItemCount / i.maxItems
		if realLevel > i.cfg.MaxLevels {
			realLevel = i.cfg.MaxLevels
		}
		if realLevel == block.Metadata().Level {
			continue
		}
		block.Metadata().Level = realLevel
		i.fs.UpdateMetadata(block)
	}

	return blocks, nil
}

func isAdjacent[T streedb.Entry](a, b *streedb.MetaFile[T]) bool {
	return a.Max.Adjacent(b.Min) || b.Max.Adjacent(a.Min)
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
