package core

import (
	"sort"
	"sync"
	"time"

	"github.com/sayden/streedb"
	"github.com/sayden/streedb/destfs"
	"github.com/sayden/streedb/fileformat"
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
	// MAX_LEVEL_0_TOTAL_BLOCKS = 1024 * 8
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

type LsmTree[T streedb.Entry] struct {
	walPool sync.Pool
	wal     Wal[T]
	fs      streedb.DestinationFs[T]
	levels  streedb.Levels[T]
}

func NewLsmTree[T streedb.Entry](initialPath string, maxWalItems int) (*LsmTree[T], error) {
	fs, levels, err := destfs.InitStartup[T](initialPath)
	if err != nil {
		panic(err)
	}
	l := &LsmTree[T]{
		walPool: sync.Pool{
			New: func() interface{} {
				return newInMemoryWal[T](maxWalItems)
			},
		},
		fs:     fs,
		levels: levels,
	}
	l.wal = l.walPool.Get().(Wal[T])

	return l, nil
}

func (l *LsmTree[T]) Append(d T) {
	if l.wal.Append(d) {
		// WAL is full, write a new block
		newBlock, err := l.wal.WriteBlock()
		if err != nil {
			log.Errorf("Error writing block: %v", err)
			return
		}
		l.levels.AppendFile(newBlock)

		l.wal = newInMemoryWal[T](cap(l.wal.GetData()))
	}
}

func (l *LsmTree[T]) WriteBlock() (streedb.Fileblock[T], error) {
	entries := l.wal.GetData()
	sort.Sort(entries)

	block, err := fileformat.NewFile(entries, 0)
	if err != nil {
		return nil, err
	}

	// reset the wal
	l.wal = l.walPool.Get().(Wal[T])

	return block, nil
}

func (l *LsmTree[T]) Find(d T) (streedb.Entry, bool, error) {
	// Look in the WAL
	if v, found := l.wal.Find(d); found {
		return v, true, nil
	}

	// Look in the meta, to open the files
	for i := 0; i <= streedb.MAX_LEVELS; i++ {
		for _, level := range l.levels.GetLevel(i) {
			if v, found, err := level.Find(d); found {
				return v, true, nil
			} else if err != nil {
				return nil, false, err
			}
		}
	}

	return nil, false, nil
}

func (l *LsmTree[T]) Close() error {
	// Close the wal
	walBlock, err := l.wal.Close()
	if err != nil {
		return err
	}

	if walBlock == nil {
		return nil
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

func (l *LsmTree[T]) Compact() error {
	overlapped := make([][2]streedb.Fileblock[T], 0)
	toAdd := make([]streedb.Fileblock[T], 0)
	toRemove := make(map[string]streedb.Fileblock[T])
	alreadyMerged := make(map[string]struct{})

	// declaring this here because it's only used in this function
	mergeAndUptadeCandidates := func(a, b streedb.Fileblock[T]) error {
		newBlock, err := a.Merge(b)
		if err != nil {
			return err
		}
		newBlock.SetLevel(b.GetLevel() + 1)

		toAdd = append(toAdd, newBlock)

		(toRemove)[a.GetID()] = a
		(toRemove)[b.GetID()] = b

		(alreadyMerged)[a.GetID()] = struct{}{}
		(alreadyMerged)[b.GetID()] = struct{}{}

		return nil

	}

	// read blocks from higher levels to lower levels
	for levelIdx := 5; levelIdx >= 1; levelIdx-- {
		higherLevel := l.levels.GetLevel(levelIdx)
		if len(higherLevel) == 0 {
			continue
		}

		lowerLevel := l.levels.GetLevel(levelIdx - 1)

	higherBlock:
		// find overlapping blocks from different levels
		for _, higherBlock := range higherLevel {
			if _, ok := alreadyMerged[higherBlock.GetID()]; ok {
				continue
			}

			for _, lowerBlock := range lowerLevel {
				if _, ok := alreadyMerged[lowerBlock.GetID()]; ok {
					continue
				}
				if hasOverlap(higherBlock, lowerBlock) {
					overlapped = append(overlapped, [2]streedb.Fileblock[T]{higherBlock, lowerBlock})
					break higherBlock
				}
			}
		}

		// merge blocks from different levels
		for _, blocks := range overlapped {
			// idx 0 always higher level, idx 1 always lower
			mergeAndUptadeCandidates(blocks[0], blocks[1])
		}
	}

	// merge blocks from the same level
	for level := 0; level < streedb.MAX_LEVELS; level++ {
		blocks := l.levels.GetLevel(level)
		for i := 0; i < len(blocks)-1; i++ {
			if _, ok := alreadyMerged[blocks[i].GetID()]; ok {
				continue
			}
			for j := i + 1; j < len(blocks); j++ {
				if _, found := alreadyMerged[blocks[j].GetID()]; found {
					continue
				}

				if hasOverlap(blocks[i], blocks[j]) {
					mergeAndUptadeCandidates(blocks[i], blocks[j])
					continue
				}

				if isSizeExceeded(blocks[i], level) && isSizeExceeded(blocks[j], level) {
					mergeAndUptadeCandidates(blocks[i], blocks[j])
					continue
				}
			}
		}
	}

	// merge blocks if level contains too many blocks
	for level := 0; level < streedb.MAX_LEVELS-1; level++ {
		blocks := l.levels.GetLevel(level)
		if len(blocks) >= MAX_LEVELS_TOTAL_BLOCKS[level] {
			for i, j := 0, 1; j < len(blocks); i, j = i+1, j+1 {
				if _, ok := alreadyMerged[blocks[i].GetID()]; ok {
					continue
				}
				if _, ok := alreadyMerged[blocks[j].GetID()]; ok {
					continue
				}

				mergeAndUptadeCandidates(blocks[0], blocks[1])
			}
		}
	}

	// remove blocks
	for _, block := range toRemove {
		l.levels.RemoveFile(block)
	}

	// add new blocks
	for _, block := range toAdd {
		l.levels.AppendFile(block)
	}

	return nil
}

func (l *LsmTree[T]) AppendFile(b streedb.Fileblock[T]) {
	l.levels.AppendFile(b)
}

func (l *LsmTree[T]) RemoveFile(b streedb.Fileblock[T]) error {
	return l.levels.RemoveFile(b)
}
