package fs

import "github.com/sayden/streedb"

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
