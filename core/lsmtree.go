package core

import (
	"errors"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
)

func NewLsmTree[E db.Entry](cfg *db.Config) (*LsmTree[E], error) {
	if cfg.LevelFilesystems == nil {
		cfg.LevelFilesystems = make([]string, 0, cfg.MaxLevels)
		for i := 0; i < cfg.MaxLevels; i++ {
			cfg.LevelFilesystems = append(cfg.LevelFilesystems, cfg.Filesystem)
		}
	}

	itemLimitPromoter := newItemLimitPromoter[E](100, cfg.MaxLevels)
	sizeLimitPromoter := newSizeLimitPromoter[E](cfg.MaxLevels)
	levels, err := fs.NewLeveledFilesystem(cfg, sizeLimitPromoter, itemLimitPromoter)
	if err != nil {
		panic(err)
	}

	l := &LsmTree[E]{
		levels: levels,
		cfg:    cfg,
	}

	// l.wal = newInMemoryWal(cfg, levels)
	l.wal = newNMMemoryWal(cfg, levels)

	l.compactor, err = NewTieredMultiFsCompactor(cfg, levels)
	if err != nil {
		panic(err)
	}

	return l, nil
}

type LsmTree[T db.Entry] struct {
	cfg *db.Config

	compactor db.Compactor[T]
	wal       db.Wal[T]
	levels    db.Levels[T]
}

func (l *LsmTree[T]) Append(d T) {
	if l.wal.Append(d) {
		// WAL is full
	}
}

func (l *LsmTree[T]) RangeIterator(begin, end T) (db.EntryIterator[T], bool, error) {
	return l.levels.RangeIterator(begin, end)
}

func (l *LsmTree[T]) ForwardIterator(d T) (db.EntryIterator[T], bool, error) {
	return l.levels.ForwardIterator(d)
}

func (l *LsmTree[T]) Find(d T) (db.Entry, bool, error) {
	// Look in the WAL
	if v, found := l.wal.Find(d); found {
		return v, true, nil
	}

	return l.levels.Find(d)
}

func (l *LsmTree[T]) Close() (err error) {
	// Close the wal and write whatever is left in it
	errs := make([]error, 0)

	if err = l.wal.Close(); err != nil {
		errs = append(errs, err)
	}

	if err = l.levels.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (l *LsmTree[T]) Compact() error {
	return l.compactor.Compact(getBlocksFromLevels(l.cfg.MaxLevels, l.levels))
}

func getBlocksFromLevels[T db.Entry](maxLevels int, levels db.Levels[T]) []*db.Fileblock[T] {
	var blocks []*db.Fileblock[T]
	for i := 0; i < maxLevels; i++ {
		level := levels.Level(i)
		blocks = append(blocks, level.Fileblocks()...)
	}

	return blocks
}
