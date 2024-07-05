package core

import (
	"errors"
	"math"

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
}

var ErrNoBlocksFound = errors.New("no blocks found")

func (mf *TieredMultiFsCompactor[T]) Compact(fileblocks []db.Fileblock[T]) error {
	if len(fileblocks) < 1 {
		return ErrNoBlocksFound
	}

	var (
		i       = 0
		j       = 1
		err     error
		a       db.Fileblock[T]
		b       db.Fileblock[T]
		entries db.Entries[T]
	)

	initialLen := len(fileblocks)
loopStart:
	for i < len(fileblocks) {
		a = fileblocks[i]
		j = i + 1

		for j < initialLen {
			b = fileblocks[j]

			// don't try to merge level 5 with level 1 blocks to reduce write amplification
			areNonAdjacentLevels := math.Abs(float64(a.Metadata().Level-b.Metadata().Level)) > 1

			if areNonAdjacentLevels {
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

				if err = mf.levels.RemoveFile(a); err != nil {
					return errors.Join(errors.New("error deleting block during compaction"), err)
				}
				if err = mf.levels.RemoveFile(b); err != nil {
					return errors.Join(errors.New("error deleting block during compaction"), err)
				}

				fileblocks = append(fileblocks[:i], fileblocks[i+1:]...)
				fileblocks = append(fileblocks[:j-1], fileblocks[j:]...)

				break loopStart
			}
			j++
		}
		i++
	}

	return nil
}
