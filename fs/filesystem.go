package fs

import (
	"github.com/sayden/streedb"
	local "github.com/sayden/streedb/fs/local"
	fss3 "github.com/sayden/streedb/fs/s3"
)

func NewFilesystem[T streedb.Entry](c *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	switch c.Filesystem {
	case streedb.FILESYSTEM_S3:
		if c.Format == streedb.FILE_FORMAT_PARQUET {
			return fss3.InitParquetS3[T](c)
		} else {
			return fss3.InitJSONS3[T](c)
		}
	default:
		if c.Format == streedb.FILE_FORMAT_PARQUET {
			return local.InitParquetLocal[T](c)
		} else {
			return local.InitJSONLocal[T](c)
		}
	}
}
