package core

import (
	"errors"
	"math"

	"github.com/emirpasic/gods/v2/sets/treeset"
	db "github.com/sayden/streedb"
)

func NewSingleLevelCompactor[T db.Entry](cfg *db.Config, fs db.Filesystem[T], level db.Level[T]) db.Compactor[T] {
	return &SingleLevelCompactor[T]{
		fs:    fs,
		level: level,
		cfg:   cfg,
	}
}

type SingleLevelCompactor[T db.Entry] struct {
	fs    db.Filesystem[T]
	level db.Level[T]
	cfg   *db.Config
}

func (s *SingleLevelCompactor[T]) Compact(fileblocks []db.Fileblock[T]) error {
	if len(fileblocks) < 1 {
		return nil
	}

	var (
		blocksToRemove = treeset.New[string]()
	)

	var (
		i       = 0
		j       = 1
		err     error
		a       db.Fileblock[T]
		b       db.Fileblock[T]
		entries db.Entries[T]
	)

	initialLen := len(fileblocks)
	for i < initialLen {
		a = fileblocks[i]
		if blocksToRemove.Contains(a.UUID()) {
			i++
			continue
		}
		j = i + 1

		for j < initialLen {
			b = fileblocks[j]

			// don't try to merge level 5 with level 1 blocks to reduce write amplification
			areNonAdjacentLevels := math.Abs(float64(a.Metadata().Level-b.Metadata().Level)) > 1

			if blocksToRemove.Contains(b.UUID()) || areNonAdjacentLevels {
				j++
				continue
			}

			if HasOverlap(a.Metadata(), b.Metadata()) || isAdjacent(a.Metadata(), b.Metadata()) {
				if entries, err = db.Merge(a, b); err != nil {
					return errors.Join(errors.New("failed to create new fileblock"), err)
				}

				meta := db.NewMetadataBuilder[T]().WithEntries(entries)
				if err = s.level.Create(entries, meta); err != nil {
					return errors.Join(errors.New("failed to create new fileblock"), err)
				}

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
	result := make([]db.Fileblock[T], 0, len(fileblocks))
	for i := 0; i < len(fileblocks); i++ {
		block := fileblocks[i]

		if blocksToRemove.Contains(block.UUID()) {
			if err = s.fs.Remove(block); err != nil {
				return errors.Join(errors.New("error deleting block during compaction"), err)
			}
			continue
		}

		// Untouched fileblock
		result = append(result, block)
	}

	return nil
}

func NewTieredSingleFsCompactor[T db.Entry](
	cfg *db.Config,
	fs db.Filesystem[T],
	levels db.Levels[T],
	promoter db.LevelPromoter[T]) db.Compactor[T] {
	return &TieredSingleFsCompactor[T]{
		cfg:      cfg,
		levels:   levels,
		fs:       fs,
		promoter: promoter,
	}
}

type TieredSingleFsCompactor[T db.Entry] struct {
	fs       db.Filesystem[T]
	cfg      *db.Config
	levels   db.Levels[T]
	promoter db.LevelPromoter[T]
}

func (t *TieredSingleFsCompactor[T]) Compact(blocks []db.Fileblock[T]) error {
	err := t.compact(blocks)
	if err != nil {
		return err
	}

	return nil
}

func (t *TieredSingleFsCompactor[T]) compact(blocks []db.Fileblock[T]) error {
	totalFileblocks := 0

	for level := 0; level < t.cfg.MaxLevels; level++ {
		totalFileblocks += len(t.levels.GetLevel(level).Fileblocks())
	}

	if totalFileblocks == 0 {
		return nil
	}

	mergedFileblocks := make([]db.Fileblock[T], 0, totalFileblocks)
	for levelIdx := 0; levelIdx < t.cfg.MaxLevels; levelIdx++ {
		level := t.levels.GetLevel(levelIdx)
		mergedFileblocks = append(mergedFileblocks, level.Fileblocks()...)
	}

	same := NewSingleLevelCompactor(t.cfg, t.fs, NewLevel(t.cfg, t.fs, mergedFileblocks))
	err := same.Compact(blocks)
	if err != nil {
		return err
	}

	return nil
}

func (t *TieredSingleFsCompactor[T]) getFilesystemForLevel(level int) db.Filesystem[T] {
	if level == 0 {
		panic("unexpected level received")
	}

	return nil
}

// Deprecated: Use NewBasicLevel instead
func NewLevel[T db.Entry](cfg *db.Config, filesystem db.Filesystem[T], data []db.Fileblock[T]) db.Level[T] {
	panic("deprecated")
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

	// level := fs.NewBasicLevel(cfg, filesystem)
	// return &fs.BasicLevel[T]{fileblocks: data, min: min, max: max}
	return nil
}
