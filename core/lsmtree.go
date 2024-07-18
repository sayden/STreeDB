package core

import (
	"cmp"
	"errors"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
	"github.com/thehivecorporation/log"
)

func NewLsmTree[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, persistStrategies ...db.WalFlushStrategy[O]) (*LsmTree[O, E], error) {
	if cfg.LevelFilesystems == nil {
		cfg.LevelFilesystems = make([]string, 0, cfg.MaxLevels)
		for i := 0; i < cfg.MaxLevels; i++ {
			cfg.LevelFilesystems = append(cfg.LevelFilesystems, cfg.Filesystem)
		}
	}

	timeLimitPromoter := newTimeLimitPromoter[O, E](cfg)
	itemLimitPromoter := newItemLimitPromoter[O, E](cfg)
	sizeLimitPromoter := newSizeLimitPromoter[O, E](cfg)
	levels, err := fs.NewLeveledFilesystem[O, E](cfg, sizeLimitPromoter, itemLimitPromoter, timeLimitPromoter)
	if err != nil {
		panic(err)
	}

	l := &LsmTree[O, E]{
		levels: levels,
		cfg:    cfg,
	}

	if persistStrategies == nil {
		persistStrategies = make([]db.WalFlushStrategy[O], 0)
	}
	// Create the WAL
	l.wal = newNMMemoryWal[O, E](cfg, levels,
		append(persistStrategies, []db.WalFlushStrategy[O]{
			newItemLimitWalFlushStrategy[O](cfg.Wal.MaxItems),
			newSizeLimitWalFlushStrategy[O](cfg.Wal.MaxSizeBytes),
		}...)...)

	compactionStrategies := &samePrimaryIndexCompactionStrategy[O]{and: &overlappingCompactionStrategy[O]{}}
	l.compactor, err = NewTieredMultiFsCompactor[O, E](cfg, levels, compactionStrategies)
	if err != nil {
		panic(err)
	}

	return l, nil
}

type LsmTree[O cmp.Ordered, E db.Entry[O]] struct {
	cfg *db.Config

	compactor db.Compactor[O]
	wal       db.Wal[O, E]
	levels    *fs.MultiFsLevels[O]
}

func (l *LsmTree[O, E]) Append(d db.Entry[O]) {
	err := l.wal.Append(d)
	if err != nil {
		log.WithError(err).Error("error appending to wal")
	}
}

func (l *LsmTree[O, E]) Find(pIdx, sIdx string, min, max O) (E, bool, error) {
	// Look in the WAL
	if v, found := l.wal.Find(pIdx, sIdx, min, max); found {
		return v, true, nil
	}

	entry, found, err := l.levels.Find(pIdx, sIdx, min, max)
	if err != nil {
		return *new(E), false, err
	}
	if !found {
		return *new(E), false, nil
	}

	e, ok := entry.(E)

	return e, ok, nil
}

func (l *LsmTree[O, E]) Close() (err error) {
	// Close the wal and write whatever is left in it
	errs := make([]error, 0)

	if err = l.wal.Close(); err != nil {
		errs = append(errs, err)
	}

	// l.levels will take care of closing every embedded level
	if err = l.levels.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (l *LsmTree[O, E]) Compact() error {
	return l.compactor.Compact(getBlocksFromLevels[O](l.cfg.MaxLevels, l.levels))
}

func getBlocksFromLevels[O cmp.Ordered](maxLevels int, levels *fs.MultiFsLevels[O]) []*db.Fileblock[O] {
	var blocks []*db.Fileblock[O]
	for i := 0; i < maxLevels; i++ {
		level := levels.Level(i)
		blocks = append(blocks, level.Fileblocks()...)
	}

	return blocks
}
