package core

import (
	"cmp"
	"errors"
	"time"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
	"github.com/thehivecorporation/log"
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
	l.wal = newNMMemoryWal(cfg, levels,
		newItemLimitWalFlushStrategy[O, E](cfg.Wal.MaxItems),
		newSizeLimitWalFlushStrategy[O, E](cfg.Wal.MaxSizeBytes),
		newTimeLimitWalFlushStrategy[O, E](time.Duration(cfg.Wal.MaxElapsedTimeMs*1000)),
	)

	compactionStrategies := &samePrimaryIndexCompactionStrategy[O]{and: &overlappingCompactionStrategy[O]{}}
	l.compactor, err = NewTieredMultiFsCompactor(cfg, levels, compactionStrategies)
	if err != nil {
		panic(err)
	}

	return l, nil
}

type LsmTree[O cmp.Ordered, E db.Entry[O]] struct {
	cfg *db.Config

	compactor db.Compactor[O, E]
	wal       db.Wal[O, E]
	levels    db.Levels[O, E]
}

func (l *LsmTree[O, E]) Append(d E) {
	err := l.wal.Append(d)
	if err != nil {
		log.WithError(err).Error("error appending to wal")
	}
}

// func (l *LsmTree[O, T]) RangeIterator(begin, end T) (db.EntryIterator[O, T], bool, error) {
// 	return l.levels.RangeIterator(begin, end)
// }
//
// func (l *LsmTree[O, T]) ForwardIterator(d T) (db.EntryIterator[O, T], bool, error) {
// 	return l.levels.ForwardIterator(d)
// }

func (l *LsmTree[O, T]) Find(d T) (db.Entry[O], bool, error) {
	// Look in the WAL
	if v, found := l.wal.Find(d); found {
		return v, true, nil
	}

	return l.levels.Find(d)
}

func (l *LsmTree[O, T]) Close() (err error) {
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

func (l *LsmTree[O, T]) Compact() error {
	return l.compactor.Compact(getBlocksFromLevels(l.cfg.MaxLevels, l.levels))
}

func getBlocksFromLevels[O cmp.Ordered, E db.Entry[O]](maxLevels int, levels db.Levels[O, E]) []*db.Fileblock[O, E] {
	var blocks []*db.Fileblock[O, E]
	for i := 0; i < maxLevels; i++ {
		level := levels.Level(i)
		blocks = append(blocks, level.Fileblocks()...)
	}

	return blocks
}
