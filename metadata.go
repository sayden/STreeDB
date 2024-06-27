package streedb

import (
	"time"
)

const (
	WRITER_PARQUET = iota
	WRITER_LOCAL_JSON
)

type Metadata[T Entry] interface {
	GetID() string

	GetMin() Entry
	GetMax() Entry

	GetSize() int64

	GetLevel() int
	SetLevel(int)

	GetBlock() *MetaFile[T]
}

type MetaFile[T Entry] struct {
	CreatedAt time.Time
	ItemCount int
	Size      int64
	Level     int
	MinVal    T
	MaxVal    T
	*FileBlockRW
}

func (b *MetaFile[T]) SetLevel(l int) {
	b.Level = l
}

func (b *MetaFile[T]) GetLevel() int {
	return b.Level
}

func (b *MetaFile[T]) GetMin() Entry {
	return b.MinVal
}

func (b *MetaFile[T]) GetMax() Entry {
	return b.MaxVal
}

func (b *MetaFile[T]) Close() error {
	b.FileBlockRW.Close()
	return nil
}

func (b *MetaFile[T]) GetID() string {
	return b.Uuid
}

func (b *MetaFile[T]) GetSize() int64 {
	return b.Size
}

// func EntryFallsInside[T Entry](b Metadata[T], d Entry) bool {
// 	return b.GetMin().LessThan(d) && d.LessThan(b.GetMax())
// }

func EntryFallsInsideMinMax(min, max, t Entry) bool {
	return min.LessThan(t) && t.LessThan(max)
}
