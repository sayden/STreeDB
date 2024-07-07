package fs

import (
	"errors"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/bplustree"
	local "github.com/sayden/streedb/fs/local"
	fss3 "github.com/sayden/streedb/fs/s3"
)

func NewLeveledFilesystem[T db.Entry](cfg *db.Config, promoter ...db.LevelPromoter[T]) (db.Levels[T], error) {
	levels := &MultiFsLevels[T]{
		cfg:                cfg,
		promoters:          promoter,
		tree:               bplustree.NewTree[T, db.Fileblock[T]](db.EntryCmp),
		fileblockListeners: make([]db.FileblockListener[T], 0, 10),
		list:               db.MapLL[T, db.Fileblock[T]]{},
	}
	// add self to the listeners
	levels.fileblockListeners = append(levels.fileblockListeners, levels)

	result := make(map[int]db.Level[T])

	if len(cfg.LevelFilesystems) == 0 {
		panic("LevelFilesystems must have at least one entry")
	}
	if cfg.MaxLevels != len(cfg.LevelFilesystems) {
		panic("MaxLevels number and LevelFilesystems lenght must be the same")
	}

	var err error

	for levelIdx, level := range cfg.LevelFilesystems {
		var fs db.Filesystem[T]

		switch db.FilesystemTypeReverseMap[level] {
		case db.FILESYSTEM_TYPE_MEMORY:
			switch db.ReverseFormatMap[cfg.Format] {
			case db.FILE_FORMAT_JSON:
				fs = local.NewMemoryFilesystem[T](cfg)
				result[levelIdx] = NewBasicLevel(cfg, fs, levels)
			default:
				return nil, db.ErrUnknownFortmaType
			}
		case db.FILESYSTEM_TYPE_LOCAL:
			switch db.ReverseFormatMap[cfg.Format] {
			case db.FILE_FORMAT_PARQUET:
				if fs, err = local.InitParquetLocal[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs, levels)
			case db.FILE_FORMAT_JSON:
				if fs, err = local.InitJSONLocal[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs, levels)
			default:
				return nil, db.ErrUnknownFortmaType
			}

		case db.FILESYSTEM_TYPE_S3:
			switch db.ReverseFormatMap[cfg.Format] {
			case db.FILE_FORMAT_PARQUET:
				if fs, err = fss3.InitParquetS3[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs, levels)
			case db.FILE_FORMAT_JSON:
				if fs, err = fss3.InitJSONS3[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs, levels)
			default:
				return nil, db.ErrUnknownFilesystemType
			}
		default:
			return nil, db.ErrUnknownFilesystemType
		}
	}

	levels.levels = result
	return levels, nil
}

type MultiFsLevels[T db.Entry] struct {
	cfg                *db.Config
	promoters          []db.LevelPromoter[T]
	levels             map[int]db.Level[T]
	tree               *bplustree.Tree[T, db.Fileblock[T]]
	list               db.MapLL[T, db.Fileblock[T]]
	fileblockListeners []db.FileblockListener[T]
}

func (b *MultiFsLevels[T]) OnNewFileblock(block db.Fileblock[T]) {
	b.list.SetMin(block.Metadata().Min, block)
}

func (b *MultiFsLevels[T]) OnFileblockRemoved(block db.Fileblock[T]) {
	b.list.Remove(block)
}

func (b *MultiFsLevels[T]) NewFileblock(es db.Entries[T], initialLevel int) error {
	meta := db.NewMetadataBuilder[T]().WithEntries(es).WithLevel(initialLevel)

	for _, promoter := range b.promoters {
		if err := promoter.Promote(meta); err != nil {
			return err
		}
	}

	_, err := b.levels[meta.Level].Create(es, meta)
	if err != nil {
		return err
	}

	return nil
}

func (b *MultiFsLevels[T]) RemoveFile(a db.Fileblock[T]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].RemoveFile(a)
}

func (b *MultiFsLevels[T]) Open(p string) (db.Fileblock[T], error) {
	return nil, errors.New("unreachable")
}

func (b *MultiFsLevels[T]) Create(es db.Entries[T], meta *db.MetadataBuilder[T]) (db.Fileblock[T], error) {
	return nil, errors.New("unreachable")
}

func (b *MultiFsLevels[T]) Find(d T) (db.Entry, bool, error) {
	// Look in the meta, to open the files
	for i := 0; i < b.cfg.MaxLevels; i++ {
		level := b.levels[i]
		if v, found, err := level.Find(d); found {
			return v, true, nil
		} else if err != nil {
			return nil, false, err
		}
	}

	return nil, false, nil
}

func (b *MultiFsLevels[T]) FindFileblock(d T) (db.Fileblock[T], bool, error) {
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

func (b *MultiFsLevels[T]) RangeIterator(begin, end T) (db.EntryIterator[T], bool, error) {
	var (
		fileblock db.Fileblock[T]
		found     bool
		err       error
	)

	for i := 0; i < b.cfg.MaxLevels; i++ {
		level := b.levels[i]
		if fileblock, found, err = level.FindFileblock(begin); found {
			break
		} else if err != nil {
			return nil, false, err
		}
	}

	iter, found := db.NewRangeIterator(&b.list, fileblock, begin, end)
	return iter, found, nil
}

func (b *MultiFsLevels[T]) ForwardIterator(d T) (db.EntryIterator[T], bool, error) {
	var (
		fileblock db.Fileblock[T]
		found     bool
		err       error
	)

	for i := 0; i < b.cfg.MaxLevels; i++ {
		level := b.levels[i]
		if fileblock, found, err = level.FindFileblock(d); found {
			break
		} else if err != nil {
			return nil, false, err
		}
	}

	iter, found := db.NewForwardIterator(&b.list, fileblock, d)
	return iter, found, nil
}

func (b *MultiFsLevels[T]) Fileblocks() []db.Fileblock[T] {
	var blocks []db.Fileblock[T]
	for i := 0; i < b.cfg.MaxLevels; i++ {
		level := b.levels[i]
		blocks = append(blocks, level.Fileblocks()...)
	}

	return blocks
}

func (b *MultiFsLevels[T]) Level(i int) db.Level[T] {
	return b.levels[i]
}

func (b *MultiFsLevels[T]) Close() error {
	for _, level := range b.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}
	b.tree.Close()

	return nil
}
