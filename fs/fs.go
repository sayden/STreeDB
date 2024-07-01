package fs

import (
	"errors"

	"github.com/sayden/streedb"
)

func NewFilesystem[T streedb.Entry](c *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	switch c.Filesystem {
	case streedb.FILESYSTEM_S3:
		if c.Format == streedb.FILE_FORMAT_PARQUET {
			return InitParquetS3[T](c)
		} else {
			return InitJSONS3[T](c)
		}
	default:
		if c.Format == streedb.FILE_FORMAT_PARQUET {
			return InitParquetLocal[T](c)
		} else {
			return InitJSONLocal[T](c)
		}
	}
}

func NewFileblockBuilder[T streedb.Entry](c *streedb.Config, fs streedb.Filesystem[T]) (streedb.FileblockBuilder[T], error) {
	switch c.Filesystem {
	case streedb.FILESYSTEM_S3:
		if c.Format == streedb.FILE_FORMAT_PARQUET {
			return func(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
				return newParquetS3Fileblock(entries, cfg, level, fs)
			}, nil
		} else {
			return func(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
				return newJSONS3Fileblock(entries, cfg, level, fs)
			}, nil
		}
	case streedb.FILESYSTEM_LOCAL:
		if c.Format == streedb.FILE_FORMAT_PARQUET {
			return func(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
				return newJSONS3Fileblock(entries, cfg, level, fs)
			}, nil
		} else {
			return func(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
				return newJSONLocalFileblock(cfg, entries, level, fs)
			}, nil
		}
	}

	return nil, errors.New("unsupported filesystem or format")
}
