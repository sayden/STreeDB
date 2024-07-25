package core

import (
	"cmp"
	"errors"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
)

func NewTieredMultiFsCompactor[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, levels *fs.MultiFsLevels[O], mergers ...db.CompactionStrategy[O]) (db.Compactor[O], error) {
	return &TieredMultiFsCompactor[O, E]{
		cfg:                cfg,
		levels:             levels,
		compactionStrategy: mergers,
	}, nil
}

// TieredMultiFsCompactor compacts fileblocks across multiple levels and filesystems.
// It is effective but it is N^2 in the number of fileblocks.
type TieredMultiFsCompactor[O cmp.Ordered, E db.Entry[O]] struct {
	cfg                *db.Config
	levels             *fs.MultiFsLevels[O]
	compactionStrategy []db.CompactionStrategy[O]
}

var ErrNoBlocksFound = errors.New("no blocks found")

func (mf *TieredMultiFsCompactor[O, E]) Compact(fileblocks []*db.Fileblock[O]) error {
	if len(fileblocks) < 1 {
		return ErrNoBlocksFound
	}

	var (
		i            = 0
		j            = 1
		err          error
		a            *db.Fileblock[O]
		b            *db.Fileblock[O]
		entries      *db.EntriesMap[O]
		builder      *db.MetadataBuilder[O]
		blocksToSkip = make(map[string]struct{})
	)

	for i < len(fileblocks) {
		a = fileblocks[i]
		if _, ok := blocksToSkip[a.Metadata().UUID()]; ok {
			i++
			continue
		}
		j = i + 1
		if a.Metadata().Level == mf.cfg.MaxLevels {
			i++
			continue
		}

	jLoop:
		for j < len(fileblocks) {
			b = fileblocks[j]
			if _, ok := blocksToSkip[b.Metadata().UUID()]; ok {
				j++
				continue
			}
			if b.Metadata().Level == mf.cfg.MaxLevels {
				j++
				continue
			}

			for _, merger := range mf.compactionStrategy {
				if !merger.ShouldMerge(a.Metadata(), b.Metadata()) {
					j++
					continue jLoop
				}
			}

			if builder, entries, err = db.Merge(a, b); err != nil {
				return errors.Join(errors.New("failed to create new fileblock"), err)
			}

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
		i++
	}

	return nil
}
