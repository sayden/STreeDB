package fs

import (
	db "github.com/sayden/streedb"
	local "github.com/sayden/streedb/fs/local"
	fss3 "github.com/sayden/streedb/fs/s3"
)

func NewLeveledFilesystem[T db.Entry](cfg *db.Config) (map[int]db.Level[T], error) {
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
		case db.FILESYSTEM_TYPE_LOCAL:
			switch db.ReverseFormatMap[cfg.Format] {
			case db.FILE_FORMAT_PARQUET:
				if fs, err = local.InitParquetLocal[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs)
			case db.FILE_FORMAT_JSON:
				if fs, err = local.InitJSONLocal[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs)
			default:
				return nil, db.ErrUnknownFortmaType
			}

		case db.FILESYSTEM_TYPE_S3:
			switch db.ReverseFormatMap[cfg.Format] {
			case db.FILE_FORMAT_PARQUET:
				if fs, err = fss3.InitParquetS3[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs)
			case db.FILE_FORMAT_JSON:
				if fs, err = fss3.InitJSONS3[T](cfg, levelIdx); err != nil {
					return nil, err
				}
				result[levelIdx] = NewBasicLevel(cfg, fs)
			default:
				return nil, db.ErrUnknownFilesystemType
			}
		default:
			return nil, db.ErrUnknownFilesystemType
		}
	}

	return result, nil
}

func NewMultiFsLevels[T db.Entry](cfg *db.Config, promoter ...db.LevelPromoter[T]) (db.Levels[T], error) {
	levels, err := NewLeveledFilesystem[T](cfg)
	if err != nil {
		return nil, err
	}

	return &MultiFsLevels[T]{
		levels:    levels,
		promoters: promoter,
	}, nil
}

// MultiFsLevels is a simple implementation of a list of Levels
type MultiFsLevels[T db.Entry] struct {
	promoters []db.LevelPromoter[T]
	levels    map[int]db.Level[T]
}

func (b *MultiFsLevels[T]) GetLevel(i int) db.Level[T] {
	return b.levels[i]
}

func (b *MultiFsLevels[T]) AppendFileblock(a db.Fileblock[T]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].AppendFileblock(a)
}

func (b *MultiFsLevels[T]) Create(es db.Entries[T], initialLevel int) error {
	meta := db.NewMetadataBuilder[T]().WithEntries(es)

	for _, promoter := range b.promoters {
		if err := promoter.Promote(meta); err != nil {
			return err
		}
	}

	return b.levels[meta.Level].Create(es, meta)
}

func (b *MultiFsLevels[T]) RemoveFile(a db.Fileblock[T]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].RemoveFile(a)
}

func (b *MultiFsLevels[T]) Close() error {
	for _, level := range b.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}

	return nil
}
