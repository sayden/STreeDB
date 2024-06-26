package streedb

import (
	"time"
)

const (
	WRITER_PARQUET = iota
	WRITER_LOCAL_JSON
)

type Block[T Entry] struct {
	CreatedAt time.Time
	ItemCount int
	Size      int64
	Level     int
	MinVal    T
	MaxVal    T
	*BlockWriters
}

func (b *Block[T]) GetLevel() int {
	return b.Level
}

func (b *Block[T]) GetCreatedAt() time.Time {
	return b.CreatedAt
}

func (b *Block[T]) GetMin() Entry {
	return b.MinVal
}

func (b *Block[T]) GetMax() Entry {
	return b.MaxVal
}

func (b *Block[T]) GetItemCount() int {
	return b.ItemCount
}

func (b *Block[T]) Close() error {
	b.BlockWriters.Close()
	return nil
}

func (b *Block[T]) GetID() string {
	return b.Uuid
}

func (b *Block[T]) GetSize() int64 {
	return b.Size
}

func EntryFallsInside[T Entry](b Metadata[T], d Entry) bool {
	return b.GetMin().LessThan(d) && d.LessThan(b.GetMax())
}
