package streedb

import (
	"time"
)

type MetaFile[T Entry] struct {
	CreatedAt time.Time
	ItemCount int
	Size      int64
	Level     int
	Min       T
	Max       T
	Uuid      string

	DataFilepath string `json:"-"`
	MetaFilepath string `json:"data"`
}

func NewMetadataBuilder[T Entry](rootPath string) MetadataBuilder[T] {
	return &metadataBuilder[T]{
		rootPath: rootPath,
		metaFile: MetaFile[T]{
			CreatedAt: time.Now(),
		}}
}
