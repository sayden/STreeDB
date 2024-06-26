package main

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"time"

	"github.com/thehivecorporation/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	NUMBER_OF_THREADS = 8
)

type parquetBlock[T Entry] struct {
	Block[T]
}

func NewParquetBlock[T Entry](data Entries[T], level int) (Metadata[T], error) {
	if data.Len() == 0 {
		return nil, errors.New("empty data")
	}

	var min, max Entry
	if data.Len() > 1 {
		min = data[0]
		max = data[data.Len()-1]
	} else if data.Len() == 1 {
		min = data[0]
		max = data[0]
	}

	blockWriters, err := NewBlockWriter(DEFAULT_DB_PATH, level)
	if err != nil {
		return nil, err
	}

	block := Block[T]{
		CreatedAt:    time.Now(),
		ItemCount:    len(data),
		Level:        level,
		MinVal:       min.(T),
		MaxVal:       max.(T),
		BlockWriters: blockWriters,
	}

	// write data to file, create a new Parquet file
	parquetWriter, err := writer.NewParquetWriterFromWriter(blockWriters.dataFile, new(T), NUMBER_OF_THREADS)
	if err != nil {
		panic(err)
	}

	for _, entry := range data {
		parquetWriter.Write(entry)
	}

	if err = parquetWriter.WriteStop(); err != nil {
		panic(err)
	}

	stat, err := blockWriters.dataFile.(*os.File).Stat()
	if err != nil {
		return nil, err
	}
	block.Size = stat.Size()

	// write metadata to file
	if err = json.NewEncoder(blockWriters.metaFile).Encode(block); err != nil {
		panic(err)
	}

	return &parquetBlock[T]{
		Block: block,
	}, nil
}

func (b *parquetBlock[T]) Find(v Entry) (Entry, bool, error) {
	if !entryFallsInside[T](b, v) {
		return nil, false, nil
	}

	entries, err := b.GetEntries()
	if err != nil {
		return nil, false, err
	}

	entry, found := entries.Find(v)
	return entry, found, nil
}

func (b *parquetBlock[T]) Merge(m Metadata[T]) (Metadata[T], error) {
	entries, err := b.GetEntries()
	if err != nil {
		return nil, err
	}

	entries2, err := m.GetEntries()
	if err != nil {
		return nil, err
	}

	dest := make(Entries[T], 0, entries.Len()+entries2.Len())
	dest = append(dest, entries...)
	dest = append(dest, entries2...)

	sort.Sort(dest)

	// TODO: optimistic creation of new block
	return NewParquetBlock(dest, b.Level+1)
}

func (b *parquetBlock[T]) GetEntries() (Entries[T], error) {
	pf, err := local.NewLocalFileReader(b.DataFilepath)
	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetReader(pf, new(T), 4)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make(Entries[T], numRows)
	err = pr.Read(&entries)
	if err != nil {
		return nil, err
	}

	log.Debugf("Reading parquet file %s with %d rows", b.DataFilepath, numRows)

	return entries, nil
}

func (b *parquetBlock[T]) Remove() error {
	b.BlockWriters.Close()

	log.Debugf("Removing parquet block %s", b.DataFilepath)
	if err := os.Remove(b.DataFilepath); err != nil {
		return err
	}

	log.Debugf("Removing parquet block's meta %s", b.MetaFilepath)
	if err := os.Remove(b.MetaFilepath); err != nil {
		return err
	}

	return nil
}
