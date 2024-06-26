package fileformat

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"time"

	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	NUMBER_OF_THREADS = 8
)

type parquetBlock[T streedb.Entry] struct {
	streedb.Block[T]
	path string
}

func NewEmptyParquetBlock[T streedb.Entry](min, max *T, filepath string) (*parquetBlock[T], error) {
	meta := &parquetBlock[T]{
		Block: streedb.Block[T]{
			BlockWriters: &streedb.BlockWriters{
				MetaFilepath: filepath,
			},
			MinVal: *min,
			MaxVal: *max,
		},
	}

	metafile, err := os.Open(meta.MetaFilepath)
	if err != nil {
		return nil, err
	}
	meta.SetMeta(metafile)

	return meta, nil
}

func NewParquetBlock[T streedb.Entry](data streedb.Entries[T], path string, level int) (*parquetBlock[T], error) {
	if data.Len() == 0 {
		return nil, errors.New("empty data")
	}

	var min, max streedb.Entry
	if data.Len() > 1 {
		min = data[0]
		max = data[data.Len()-1]
	} else if data.Len() == 1 {
		min = data[0]
		max = data[0]
	}

	blockWriters, err := streedb.NewBlockWriter(path, level)
	if err != nil {
		return nil, err
	}

	block := streedb.Block[T]{
		CreatedAt:    time.Now(),
		ItemCount:    len(data),
		Level:        level,
		MinVal:       min.(T),
		MaxVal:       max.(T),
		BlockWriters: blockWriters,
	}

	// write data to file, create a new Parquet file
	parquetWriter, err := writer.NewParquetWriterFromWriter(blockWriters.GetData(), new(T), NUMBER_OF_THREADS)
	if err != nil {
		panic(err)
	}

	for _, entry := range data {
		parquetWriter.Write(entry)
	}

	if err = parquetWriter.WriteStop(); err != nil {
		panic(err)
	}

	stat, err := blockWriters.GetData().(*os.File).Stat()
	if err != nil {
		return nil, err
	}
	block.Size = stat.Size()

	// write metadata to file
	if err = json.NewEncoder(blockWriters.GetMeta()).Encode(block); err != nil {
		panic(err)
	}

	return &parquetBlock[T]{
		Block: block,
		path:  path,
	}, nil
}

func (b *parquetBlock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInside[T](b, v) {
		return nil, false, nil
	}

	entries, err := b.GetEntries()
	if err != nil {
		return nil, false, err
	}

	entry, found := entries.Find(v)
	return entry, found, nil
}

func (b *parquetBlock[T]) Merge(m streedb.Metadata[T]) (streedb.Metadata[T], error) {
	entries, err := b.GetEntries()
	if err != nil {
		return nil, err
	}

	entries2, err := m.GetEntries()
	if err != nil {
		return nil, err
	}

	dest := make(streedb.Entries[T], 0, entries.Len()+entries2.Len())
	dest = append(dest, entries...)
	dest = append(dest, entries2...)

	sort.Sort(dest)

	// TODO: optimistic creation of new block
	return NewParquetBlock(dest, b.path, b.Level+1)
}

func (b *parquetBlock[T]) GetEntries() (streedb.Entries[T], error) {
	pf, err := local.NewLocalFileReader(b.DataFilepath)
	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetReader(pf, new(T), 4)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make(streedb.Entries[T], numRows)
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
