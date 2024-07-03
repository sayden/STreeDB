package core

import (
	"errors"
	"math"

	"github.com/emirpasic/gods/v2/sets/treeset"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
)

func NewTieredMultiFsCompactor[T db.Entry](cfg *db.Config, levels db.Levels[T]) (db.Compactor[T], error) {
	levels_ := levels.(*fs.MultiFsLevels[T])

	return &TieredMultiFsCompactor[T]{
		cfg:    cfg,
		levels: levels_,
	}, nil
}

type TieredMultiFsCompactor[T db.Entry] struct {
	cfg    *db.Config
	levels *fs.MultiFsLevels[T]
	blocks []db.Fileblock[T]
}

var ErrNoBlocksFound = errors.New("no blocks found")

func (mf *TieredMultiFsCompactor[T]) Compact(fileblocks []db.Fileblock[T]) error {
	if len(fileblocks) < 1 {
		return ErrNoBlocksFound
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

				higherLevel := a.Metadata().Level
				if b.Metadata().Level > higherLevel {
					higherLevel = b.Metadata().Level
				}

				// Write the new block to its new storage directly
				if err = mf.levels.Create(entries, higherLevel); err != nil {
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
	for i := 0; i < len(fileblocks); i++ {
		block := fileblocks[i]

		if blocksToRemove.Contains(block.UUID()) {
			if err = mf.levels.RemoveFile(block); err != nil {
				return errors.Join(errors.New("error deleting block during compaction"), err)
			}
			continue
		}
	}

	return nil
}

func (mf *TieredMultiFsCompactor[T]) getFsFromLevel(level int) db.Filesystem[T] {
	fs := mf.levels.GetLevel(level).(*fs.BasicLevel[T])
	return fs.Filesystem
}
