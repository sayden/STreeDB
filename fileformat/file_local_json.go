package fileformat

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"time"

	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

type LocalBlockJSON[T streedb.Entry] struct {
	streedb.MetaFile[T]
	path string
}

func NewEmptyJSONBlock[T streedb.Entry](min, max *T, filepath string) (*LocalBlockJSON[T], error) {
	meta := &LocalBlockJSON[T]{
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

func NewJSONBlock[T streedb.Entry](data streedb.Entries[T], level int) (*LocalBlockJSON[T], error) {
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

	uuid := streedb.NewUUID() + ".jsondata"
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
	dataFile := blockWriters.GetData()
	if err = json.NewEncoder(dataFile).Encode(data); err != nil {
		panic(err)
	}

	stat, err := dataFile.(*os.File).Stat()
	if err != nil {
		return nil, err
	}
	block.Size = stat.Size()

	// write metadata to file
	if err = json.NewEncoder(blockWriters.GetMeta()).Encode(block); err != nil {
		panic(err)
	}

	return &LocalBlockJSON[T]{
		MetaFile: block,
	}, nil

}

func (l *LocalBlockJSON[T]) GetEntries() (streedb.Entries[T], error) {
	log.Debugf("Reading JSON file %s ", l.DataFilepath)

	f, err := os.Open(l.DataFilepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries := make(streedb.Entries[T], 0)
	if err = json.NewDecoder(f).Decode(&entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func (l *LocalBlockJSON[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
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

func (l *LocalBlockJSON[T]) Merge(a streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
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

func (l *LocalBlockJSON[T]) Remove() error {
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

func (l *LocalBlockJSON[T]) GetBlock() *streedb.MetaFile[T] {
	return &l.MetaFile
}
