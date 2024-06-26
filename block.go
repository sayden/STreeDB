package main

import (
	"fmt"
	"os"
	"path"
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

func NewBlockWriter(defaultFolder string, l int) (bfs *BlockWriters, err error) {
	bfs = &BlockWriters{}

	bfs.Uuid = newUUID()

	bfs.DataFilepath = path.Join(defaultFolder, fmt.Sprintf("%02d", l), bfs.Uuid)
	dataFile, err := os.Create(bfs.DataFilepath)
	if err != nil {
		return nil, err
	}
	bfs.dataFile = dataFile

	bfs.MetaFilepath = path.Join(defaultFolder, fmt.Sprintf("%02d", l), "meta_"+bfs.Uuid+".json")
	bfs.metaFile, err = os.Create(bfs.MetaFilepath)
	if err != nil {
		return nil, err
	}

	return
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
	return b.BlockWriters.Uuid
}

func (b *Block[T]) GetSize() int64 {
	return b.Size
}

func fallsInside[T Entry](b Metadata[T], d T) bool {
	return b.GetMin().LessThan(d) && d.LessThan(b.GetMax())
}

func entryFallsInside[T Entry](b Metadata[T], d Entry) bool {
	return b.GetMin().LessThan(d) && d.LessThan(b.GetMax())
}
