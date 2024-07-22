package core

import (
	"cmp"
	"errors"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
)

func NewLsmTree[O cmp.Ordered, E db.Entry[O]](cfg *db.Config) (*LsmTree[O, E], error) {
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

	// Create the WAL
	l.wal = newNMMemoryWal[O](cfg, levels,
		newItemLimitWalFlushStrategy[O](cfg.Wal.MaxItems),
		newSizeLimitWalFlushStrategy[O](cfg.Wal.MaxSizeBytes),
	)

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
	wal       db.Wal[O]
	levels    *fs.MultiFsLevels[O]
}

func (l *LsmTree[O, _]) Append(d db.Entry[O]) error {
	return l.wal.Append(d)
}

func (l *LsmTree[O, E]) Find(pIdx, sIdx string, min, max O) (db.EntryIterator[O], bool, error) {
	// Look in the WAL
	walIter, walFound := l.wal.Find(pIdx, sIdx, min, max)

	dbIter, dbFound, err := l.levels.FindSingle(pIdx, sIdx, min, max)
	if err != nil {
		return nil, false, err
	}
	if walFound && dbFound {
		walIter = db.NewIteratorMerger[O](walIter, dbIter)
		return walIter, true, nil
	}

	if walFound {
		return walIter, true, nil
	}

	return dbIter, dbFound, nil
}

func (l *LsmTree[_, _]) Close() (err error) {
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

func (l *LsmTree[_, _]) Compact() error {
	return l.compactor.Compact(l.levels.Fileblocks())
}
