package fs

import (
	db "github.com/sayden/streedb"
	local "github.com/sayden/streedb/fs/local"
	fss3 "github.com/sayden/streedb/fs/s3"
)

func NewFilesystem[T db.Entry](c *db.Config) (db.Filesystem[T], db.Levels[T], error) {
	fs, err := newFilesystem[T](c, db.FilesystemTypeReverseMap[c.Filesystem])
	if err != nil {
		return nil, nil, err
	}

	levels, err := fs.OpenAllMetaFiles()
	return fs, levels, err
}

func newFilesystem[T db.Entry](cfg *db.Config, t db.FilesystemType) (db.Filesystem[T], error) {
	switch t {
	case db.FILESYSTEM_TYPE_S3:
		if db.ReverseFormatMap[cfg.Format] == db.FILE_FORMAT_PARQUET {
			return fss3.InitParquetS3[T](cfg, 0)
		} else {
			return fss3.InitJSONS3[T](cfg, 0)
		}
	case db.FILESYSTEM_TYPE_LOCAL:
		if db.ReverseFormatMap[cfg.Format] == db.FILE_FORMAT_PARQUET {
			return local.InitParquetLocal[T](cfg, 0)
		} else {
			return local.InitJSONLocal[T](cfg, 0)
		}
	default:
		panic("Unknown filesystem type")
	}
}
