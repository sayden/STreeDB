package core

import (
	"errors"
	"sort"
	"sync"
	"time"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
	"github.com/thehivecorporation/log"
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

func NewLsmTree[T db.Entry](cfg *db.Config) (*LsmTree[T], error) {
	if cfg.LevelFilesystems == nil {
		cfg.LevelFilesystems = make([]string, 0, cfg.MaxLevels)
		for i := 0; i < cfg.MaxLevels; i++ {
			cfg.LevelFilesystems = append(cfg.LevelFilesystems, cfg.Filesystem)
		}
	}

	promoter := NewItemLimitPromoter[T](7, cfg.MaxLevels)
	levels, err := fs.NewMultiFsLevels(cfg, promoter)
	if err != nil {
		panic(err)
	}

	l := &LsmTree[T]{
		walPool: sync.Pool{
			New: func() interface{} {
				return newInMemoryWal[T](cfg)
			},
		},
		levels: levels,
		cfg:    cfg,
	}

	l.wal = l.walPool.Get().(db.Wal[T])

	l.compactor, err = NewTieredMultiFsCompactor(cfg, levels)
	if err != nil {
		panic(err)
	}

	return l, nil
}

type LsmTree[T db.Entry] struct {
	compactor db.Compactor[T]

	walPool sync.Pool
	wal     db.Wal[T]
	levels  db.Levels[T]
	cfg     *db.Config
}

func (l *LsmTree[T]) Append(d T) {
	if l.wal.Append(d) {
		// WAL is full, write a new block
		err := l.WriteBlock()
		if err != nil {
			log.Errorf("Error writing block: %v", err)
			return
		}

		l.wal = newInMemoryWal[T](l.cfg)
	}
}

func (l *LsmTree[T]) WriteBlock() (err error) {
	entries := l.wal.GetData()
	if len(entries) == 0 {
		return
	}

	sort.Sort(entries)

	if err = l.levels.Create(entries, 0); err != nil {
		return err
	}

	// reset the wal
	l.wal = l.walPool.Get().(db.Wal[T])

	return
}

func (l *LsmTree[T]) Find(d T) (db.Entry, bool, error) {
	log.WithField("key", d).Debugf("Looking for key in LSM tree")

	// Look in the WAL
	if v, found := l.wal.Find(d); found {
		return v, true, nil
	}

	// Look in the meta, to open the files
	for i := 0; i < l.cfg.MaxLevels; i++ {
		level := l.levels.GetLevel(i)
		if v, found, err := level.Find(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, err
		}
	}

	return nil, false, nil
}

func (l *LsmTree[T]) Close() (err error) {
	// Close the wal and write whatever is left in it
	errs := make([]error, 0)

	if err = l.wal.Close(); err != nil {
		errs = append(errs, err)
	}

	if err = l.WriteBlock(); err != nil {
		errs = append(errs, err)
	}

	if err = l.levels.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (l *LsmTree[T]) RemoveFile(b db.Fileblock[T]) error {
	return l.levels.RemoveFile(b)
}

func (l *LsmTree[T]) Compact() error {
	return l.compactor.Compact(getBlocksFromLevels(l.cfg.MaxLevels, l.levels))
}

func getBlocksFromLevels[T db.Entry](maxLevels int, levels db.Levels[T]) []db.Fileblock[T] {
	var blocks []db.Fileblock[T]
	for i := 0; i < maxLevels; i++ {
		level := levels.GetLevel(i)
		blocks = append(blocks, level.Fileblocks()...)
	}

	return blocks
}
