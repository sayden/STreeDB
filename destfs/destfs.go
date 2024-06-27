package destfs

import "github.com/sayden/streedb"

type DEST_FS int

const (
	DEST_FS_LOCAL = iota
	DEST_FS_S3
)

const DEFAULT_DEST_FS = DEST_FS_S3

func InitStartup[T streedb.Entry](p string, d DEST_FS) (streedb.DestinationFs[T], streedb.Levels[T], error) {
	switch d {
	case DEST_FS_LOCAL:
		return InitLocal[T](p)
	case DEST_FS_S3:
		return InitS3[T](p)
	}

	return InitLocal[T](p)
}
