package fs

import (
	"cmp"
	"errors"

	db "github.com/sayden/streedb"
	local "github.com/sayden/streedb/fs/local"
	fss3 "github.com/sayden/streedb/fs/s3"
)

func NewLeveledFilesystem[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, promoter ...db.LevelPromoter[O]) (*MultiFsLevels[O, E], error) {
	levels := &MultiFsLevels[O, E]{
		cfg:                cfg,
		promoters:          promoter,
		fileblockListeners: make([]db.FileblockListener[O, E], 0, 10),
		// list:               db.MapDLL[O, E, *db.Fileblock[O, E]]{},
		index: db.NewBtreeIndex(2, db.LLFComp[O]),
	}
	// add self to the listeners
	levels.fileblockListeners = append(levels.fileblockListeners, levels)

	result := make(map[int]*BasicLevel[O, E])

	if len(cfg.LevelFilesystems) == 0 {
		panic("LevelFilesystems must have at least one entry")
	}
	if cfg.MaxLevels != len(cfg.LevelFilesystems) {
		panic("MaxLevels number and LevelFilesystems lenght must be the same")
	}

	var err error

	for levelIdx, level := range cfg.LevelFilesystems {
		var fs db.Filesystem[O, E]

		switch db.FilesystemTypeReverseMap[level] {
		case db.FILESYSTEM_TYPE_LOCAL:
			if fs, err = local.InitParquetLocal[O, E](cfg, levelIdx); err != nil {
				return nil, err
			}
			result[levelIdx] = NewBasicLevel(cfg, fs, levels)

		case db.FILESYSTEM_TYPE_S3:
			if fs, err = fss3.InitParquetS3[O, E](cfg, levelIdx); err != nil {
				return nil, err
			}
			result[levelIdx] = NewBasicLevel(cfg, fs, levels)
		default:
			return nil, db.ErrUnknownFilesystemType
		}
	}

	levels.levels = result
	return levels, nil
}

type MultiFsLevels[O cmp.Ordered, E db.Entry[O]] struct {
	cfg                *db.Config
	promoters          []db.LevelPromoter[O]
	levels             map[int]*BasicLevel[O, E]
	index              *db.BtreeWrapper[O]
	fileblockListeners []db.FileblockListener[O, E]
}

func (b *MultiFsLevels[O, T]) OnFileblockCreated(block *db.Fileblock[O, T]) {
	b.index.Upsert(*block.Metadata().Min, block)
}

func (b *MultiFsLevels[O, T]) OnFileblockRemoved(block *db.Fileblock[O, T]) {
	b.index.Remove(*block.Metadata().Min, block)
}

func (b *MultiFsLevels[O, E]) NewFileblock(es db.EntriesMap[O, E], builder *db.MetadataBuilder[O]) error {
	for _, secIdx := range es.SecondaryIndices() {
		entry := es.Get(secIdx)
		entry.Sort()
		builder.WithEntry(entry)
	}

	for _, promoter := range b.promoters {
		if err := promoter.Promote(builder); err != nil {
			return errors.Join(errors.New("failed during promotion"), err)
		}
	}

	_, err := b.levels[builder.Level].Create(es, builder)
	if err != nil {
		return errors.Join(errors.New("failed to create new fileblock in mfs"), err)
	}

	return nil
}

func (b *MultiFsLevels[O, T]) RemoveFile(a *db.Fileblock[O, T]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].RemoveFile(a)
}

func (b *MultiFsLevels[O, T]) Open(p string) (*db.Fileblock[O, T], error) {
	return nil, errors.New("unreachable")
}

func (b *MultiFsLevels[O, E]) Create(es db.EntriesMap[O, E], meta *db.MetadataBuilder[O]) (*db.Fileblock[O, E], error) {
	return nil, errors.New("unreachable")
}

func (b *MultiFsLevels[O, E]) Find(pIdx, sIdx string, min, max O) (db.Entry[O], bool, error) {
	// ll, found := b.index.AscendRange(pIdx, sIdx, min, max)
	// ll, found := b.index.Get(min)
	// if !found {
	// 	return nil, false, nil
	// }
	// var targetEntry db.Comparable[O]
	// ll.Each(func(i int, e db.Comparable[O]) bool {
	// 	found = e.PrimaryIndex() == pIdx && e.SecondaryIndex() == sIdx
	// 	if found {
	// 		targetEntry = e
	// 		return true
	// 	}
	//
	// 	return false
	// })
	//
	// return targetEntry.(db.Entry[O]), targetEntry != nil, nil

	return nil, false, errors.New("not implemented")
}

func (b *MultiFsLevels[O, E]) FindFileblock(d E) (*db.Fileblock[O, E], bool, error) {
	for i := 0; i < b.cfg.MaxLevels; i++ {
		level := b.levels[i]
		if v, found, err := level.FindFileblock(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// func (b *MultiFsLevels[O,T]) RangeIterator(begin, end T) (db.EntryIterator[O,T], bool, error) {
// var (
// fileblock *db.Fileblock[O,T]
// found bool
// err       error
// )

// for i := 0; i < b.cfg.MaxLevels; i++ {
// 	level := b.levels[i]
// 	if fileblock, found, err = level.FindFileblock(begin); found {
// 		break
// 	} else if err != nil {
// 		return nil, false, err
// 	}
// }

// 	iter, found := db.NewRangeIterator(&b.list, begin, end)
// 	return iter, found, nil
// }

// func (b *MultiFsLevels[O,T]) ForwardIterator(d T) (db.EntryIterator[O,T], bool, error) {
// var (
// 	fileblock *db.Fileblock[O,T]
// 	found     bool
// 	err       error
// )
//
// for i := 0; i < b.cfg.MaxLevels; i++ {
// 	level := b.levels[i]
// 	if fileblock, found, err = level.FindFileblock(d); found {
// 		break
// 	} else if err != nil {
// 		return nil, false, err
// 	}
// }

// 	iter, found := db.NewForwardIterator(&b.list, d)
// 	return iter, found, nil
// }

func (b *MultiFsLevels[O, T]) Fileblocks() []*db.Fileblock[O, T] {
	var blocks []*db.Fileblock[O, T]
	for i := 0; i < b.cfg.MaxLevels; i++ {
		level := b.levels[i]
		blocks = append(blocks, level.Fileblocks()...)
	}

	return blocks
}

func (b *MultiFsLevels[O, T]) Level(i int) *BasicLevel[O, T] {
	return b.levels[i]
}

func (b *MultiFsLevels[O, T]) Close() error {
	for _, level := range b.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}

	return nil
}
