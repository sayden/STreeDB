package streedb

import (
	"cmp"
	"errors"
)

type FilesystemType int

const (
	PARQUET_NUMBER_OF_THREADS = 8
)

const (
	FILESYSTEM_TYPE_LOCAL FilesystemType = iota
	FILESYSTEM_TYPE_S3
	FILESYSTEM_TYPE_MEMORY
)

var ErrUnknownFilesystemType = errors.New("unknown filesystem type")

var FilesystemTypeMap = map[FilesystemType]string{
	FILESYSTEM_TYPE_LOCAL: "local",
	FILESYSTEM_TYPE_S3:    "s3",
}

var FilesystemTypeReverseMap = map[string]FilesystemType{
	"local": FILESYSTEM_TYPE_LOCAL,
	"s3":    FILESYSTEM_TYPE_S3,
}

type Filesystem[O cmp.Ordered, T Entry[O]] interface {
	Create(cfg *Config, entries Entries[O, T], builder *MetadataBuilder[O], listeners []FileblockListener[O, T]) (*Fileblock[O, T], error)
	FillMetadataBuilder(meta *MetadataBuilder[O]) *MetadataBuilder[O]
	Load(*Fileblock[O, T]) (Entries[O, T], error)
	OpenMetaFilesInLevel([]FileblockListener[O, T]) error
	Remove(*Fileblock[O, T], []FileblockListener[O, T]) error
	UpdateMetadata(*Fileblock[O, T]) error
}
