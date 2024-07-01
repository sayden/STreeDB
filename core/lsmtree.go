package core

import (
	"sort"
	"sync"
	"time"

	"github.com/sayden/streedb"
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

func NewLsmTree[T streedb.Entry](c *streedb.Config) (*LsmTree[T], error) {
	filesystem, levels, err := fs.NewFilesystem[T](c)
	if err != nil {
		panic(err)
	}

	l := &LsmTree[T]{
		walPool: sync.Pool{
			New: func() interface{} {
				return newInMemoryWal[T](c)
			},
		},
		fs:     filesystem,
		levels: levels,
		cfg:    c,
	}

	l.wal = l.walPool.Get().(Wal[T])
	fileblockBuilder, err := fs.NewFileblockBuilder[T](c, filesystem)
	if err != nil {
		return nil, err
	}

	// TODO: Passing the levels here feels a bit hacky
	l.compactor = NewTieredCompactor(c, fileblockBuilder, levels)

	return l, nil
}

type LsmTree[T streedb.Entry] struct {
	compactor streedb.MultiLevelCompactor[T]

	walPool sync.Pool
	wal     Wal[T]
	fs      streedb.Filesystem[T]
	levels  streedb.Levels[T]
	cfg     *streedb.Config
}

func (l *LsmTree[T]) Append(d T) {
	if l.wal.Append(d) {
		// WAL is full, write a new block
		newBlock, err := l.WriteBlock()
		if err != nil {
			log.Errorf("Error writing block: %v", err)
			return
		}
		l.levels.AppendFile(newBlock)

		l.wal = newInMemoryWal[T](l.cfg)
	}
}

func (l *LsmTree[T]) WriteBlock() (streedb.Fileblock[T], error) {
	entries := l.wal.GetData()
	sort.Sort(entries)

	block, err := l.fs.Create(entries, 0)
	if err != nil {
		return nil, err
	}

	// reset the wal
	l.wal = l.walPool.Get().(Wal[T])

	return block, nil
}

func (l *LsmTree[T]) Find(d T) (streedb.Entry, bool, error) {
	log.WithField("key", d).Debugf("Looking for key in LSM tree")

	// Look in the WAL
	if v, found := l.wal.Find(d); found {
		return v, true, nil
	}

	// Look in the meta, to open the files
	for i := 0; i <= l.cfg.MaxLevels; i++ {
		for _, fileblock := range l.levels.GetLevel(i) {
			if v, found, err := fileblock.Find(d); found {
				return v, true, nil
			} else if err != nil {
				return nil, false, err
			}
		}
	}

	return nil, false, nil
}

func (l *LsmTree[T]) Close() error {
	// Close the wal and write whatever is left in it
	errs := make([]error, 0)

	_, err := l.wal.Close()
	if err != nil {
		errs = append(errs, err)
	}

	fileblock, err := l.WriteBlock()
	if err != nil {
		errs = append(errs, err)
	} else {
		l.levels.AppendFile(fileblock)
	}

	for i := 0; i <= 5; i++ {
		for _, level := range l.levels.GetLevel(i) {
			if err := level.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (l *LsmTree[T]) AppendFile(b streedb.Fileblock[T]) {
	l.levels.AppendFile(b)
}

func (l *LsmTree[T]) RemoveFile(b streedb.Fileblock[T]) error {
	return l.levels.RemoveFile(b)
}

func (l *LsmTree[T]) Compact() error {
	levels, err := l.compactor.Compact()
	if err != nil || levels == nil {
		return err
	}

	l.levels = levels

	return nil
}
