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

	DataFilepath string `json:"Datafile"`
	MetaFilepath string `json:"Metafile"`
}

func (m *MetaFile[T]) Metadata() *MetaFile[T] {
	return m
}

func (m *MetaFile[T]) UUID() string {
	return m.Uuid
}
