package streedb

import "errors"

type FilesystemType int

const (
	FILESYSTEM_TYPE_LOCAL FilesystemType = iota
	FILESYSTEM_TYPE_S3
	FILESYSTEM_TYPE_MEMORY
)

var ErrUnknownFilesystemType = errors.New("unknown filesystem type")
var ErrUnknownFortmaType = errors.New("unknown format type")

var FilesystemTypeMap = map[FilesystemType]string{
	FILESYSTEM_TYPE_LOCAL:  "local",
	FILESYSTEM_TYPE_S3:     "s3",
	FILESYSTEM_TYPE_MEMORY: "memory",
}

var FilesystemTypeReverseMap = map[string]FilesystemType{
	"local":  FILESYSTEM_TYPE_LOCAL,
	"s3":     FILESYSTEM_TYPE_S3,
	"memory": FILESYSTEM_TYPE_MEMORY,
}

type Filesystem[T Entry] interface {
	Create(cfg *Config, entries Entries[T], metadata *MetaFile[T], listeners []FileblockListener[T]) (*Fileblock[T], error)
	FillMetadataBuilder(meta *MetadataBuilder[T]) *MetadataBuilder[T]
	Load(*Fileblock[T]) (Entries[T], error)
	OpenMetaFilesInLevel([]FileblockListener[T]) error
	Remove(*Fileblock[T], []FileblockListener[T]) error
	UpdateMetadata(*Fileblock[T]) error
}
