package core

import (
	"errors"
	"math"

	db "github.com/sayden/streedb"
)

func NewTieredMultiFsCompactor[T db.Entry](cfg *db.Config, levels db.Levels[T]) (db.Compactor[T], error) {
	return &TieredMultiFsCompactor[T]{
		cfg:    cfg,
		levels: levels,
	}, nil
}

type TieredMultiFsCompactor[T db.Entry] struct {
	cfg    *db.Config
	levels db.Levels[T]
}

var ErrNoBlocksFound = errors.New("no blocks found")

func (mf *TieredMultiFsCompactor[T]) Compact(fileblocks []*db.Fileblock[T]) error {
	if len(fileblocks) < 1 {
		return ErrNoBlocksFound
	}

	var (
		i            = 0
		j            = 1
		err          error
		a            *db.Fileblock[T]
		b            *db.Fileblock[T]
		entries      db.Entries[T]
		blocksToSkip = make(map[string]struct{})
	)

	for i < len(fileblocks) {
		a = fileblocks[i]
		if _, ok := blocksToSkip[a.Metadata().UUID()]; ok {
			i++
			continue
		}
		j = i + 1

		for j < len(fileblocks) {
			b = fileblocks[j]
			if _, ok := blocksToSkip[b.Metadata().UUID()]; ok {
				j++
				continue
			}

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
				builder := db.NewMetadataBuilder[T]().
					WithLevel(higherLevel).
					WithEntries(entries).
					WithSize(a.Size + b.Size)
				if err = mf.levels.NewFileblock(entries, builder); err != nil {
					return errors.Join(errors.New("failed to create new fileblock"), err)
				}

				if err = mf.levels.RemoveFile(a); err != nil {
					return errors.Join(errors.New("error deleting block during compaction"), err)
				}
				if err = mf.levels.RemoveFile(b); err != nil {
					return errors.Join(errors.New("error deleting block during compaction"), err)
				}

				blocksToSkip[a.Metadata().UUID()] = struct{}{}
				blocksToSkip[b.Metadata().UUID()] = struct{}{}

				break
			}
			j++
		}
		i++
	}

	return nil
}
