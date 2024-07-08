package core

import (
	"errors"
	"time"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
)

var (
	MAX_LEVELS_TOTAL_BLOCKS = [6]int{
		1,
		1024 * 4,
		1024 * 2,
		1024,
		512,
		256,
	}
)

const (
	MAX_LEVEL_0_TOTAL_BLOCKS = 1024 * 8
	MAX_LEVEL_0_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_0_BLOCK_AGE    = 1 * time.Hour

	MAX_LEVEL_1_TOTAL_BLOCKS = 1024 * 4
	MAX_LEVEL_1_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_1_BLOCK_AGE    = 24 * time.Hour

	MAX_LEVEL_2_TOTAL_BLOCKS = 1024 * 2
	MAX_LEVEL_2_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_2_BLOCK_AGE    = 24 * 7 * time.Hour

	MAX_LEVEL_3_TOTAL_BLOCKS = 1024
	MAX_LEVEL_3_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_3_BLOCK_AGE    = 24 * 15 * time.Hour

	MAX_LEVEL_4_TOTAL_BLOCKS = 512
	MAX_LEVEL_4_BLOCK_SIZE   = 1024 * 32
	MAX_LEVEL_4_BLOCK_AGE    = 24 * 30 * time.Hour
)

func NewLsmTree[E db.Entry](cfg *db.Config) (*LsmTree[E], error) {
	if cfg.LevelFilesystems == nil {
		cfg.LevelFilesystems = make([]string, 0, cfg.MaxLevels)
		for i := 0; i < cfg.MaxLevels; i++ {
			cfg.LevelFilesystems = append(cfg.LevelFilesystems, cfg.Filesystem)
		}
	}

	promoter := newItemLimitPromoter[E](7, cfg.MaxLevels)
	levels, err := fs.NewLeveledFilesystem(cfg, promoter)
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
