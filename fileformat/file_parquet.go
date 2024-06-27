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
	streedb.MetaFile[T]
}

func NewEmptyParquetBlock[T streedb.Entry](min, max *T, filepath string) (*parquetBlock[T], error) {
	meta := &parquetBlock[T]{
		MetaFile: streedb.MetaFile[T]{
			FileBlockRW: &streedb.FileBlockRW{
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

func NewParquetBlock[T streedb.Entry](data streedb.Entries[T], level int) (*parquetBlock[T], error) {
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

	uuid := streedb.NewUUID() + ".parquet"
	blockWriters, err := streedb.NewBlockWriter(uuid, level)
	if err != nil {
		return nil, err
	}

	block := streedb.MetaFile[T]{
		CreatedAt:   time.Now(),
		ItemCount:   len(data),
		Level:       level,
		MinVal:      min.(T),
		MaxVal:      max.(T),
		FileBlockRW: blockWriters,
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
		MetaFile: block,
	}, nil
}

func (l *parquetBlock[T]) GetEntries() (streedb.Entries[T], error) {
	pf, err := local.NewLocalFileReader(l.DataFilepath)
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

	log.Debugf("Reading parquet file %s with %d rows", l.DataFilepath, numRows)

	return entries, nil
}

func (l *parquetBlock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInside(l, v) {
		return nil, false, nil
	}

	entries, err := l.GetEntries()
	if err != nil {
		return nil, false, err
	}

	entry, found := entries.Find(v)
	return entry, found, nil
}

func (l *parquetBlock[T]) Merge(a streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	entries, err := l.GetEntries()
	if err != nil {
		return nil, err
	}

	entries2, err := a.GetEntries()
	if err != nil {
		return nil, err
	}

	dest := make(streedb.Entries[T], 0, entries.Len()+entries2.Len())
	dest = append(dest, entries...)
	dest = append(dest, entries2...)

	sort.Sort(dest)

	// TODO: optimistic creation of new block
	return NewFile(dest, l.Level+1)
}

func (l *parquetBlock[T]) Remove() error {
	l.FileBlockRW.Close()

	log.Debugf("Removing parquet block %s", l.DataFilepath)
	if err := os.Remove(l.DataFilepath); err != nil {
		return err
	}

	log.Debugf("Removing parquet block's meta %s", l.MetaFilepath)
	if err := os.Remove(l.MetaFilepath); err != nil {
		return err
	}

	return nil
}

func (l *parquetBlock[T]) GetBlock() *streedb.MetaFile[T] {
	return &l.MetaFile
}
