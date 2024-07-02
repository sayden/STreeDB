package fslocal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

// InitJSONLocal initializes a local filesystem destination. Writes the folder structure if required
// and then read the medatada files that are already there.
func InitJSONLocal[T streedb.Entry](cfg *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	return initLocal[T](cfg, jSONFsBuilder)
}

type localJSONFs[T streedb.Entry] struct {
	streedb.Filesystem[T] // Implements

	cfg *streedb.Config
}

func (f *localJSONFs[T]) Open(p string) (meta *streedb.MetaFile[T], err error) {
	return open[T](p)
}

func (f *localJSONFs[T]) Merge(a, b streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	newEntries, err := streedb.Merge(a, b)
	if err != nil {
		return nil, err
	}
	return f.Create(f.cfg, newEntries, a.Metadata().Level)
}

func (f *localJSONFs[T]) Load(b streedb.Fileblock[T]) (streedb.Entries[T], error) {
	file, err := os.Open(b.Metadata().DataFilepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries := make(streedb.Entries[T], 0)
	if err = json.NewDecoder(file).Decode(&entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func (f *localJSONFs[T]) Create(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](f.cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".jsondata").
		Build()
	if err != nil {
		return nil, errors.Join(errors.New("error creating metadata: "), err)
	}

	dataFile, err := os.Create(meta.DataFilepath)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("error creating data file '%s' on local FS: ", meta.DataFilepath), err)
	}
	defer dataFile.Close()

	if err = json.NewEncoder(dataFile).Encode(entries); err != nil {
		log.WithFields(log.Fields{"meta_file": meta.MetaFilepath, "data_file": meta.DataFilepath}).Warn("error happened during creating of fileblock, removing files")
		os.Remove(meta.DataFilepath)
		os.Remove(meta.MetaFilepath)
		return nil, err
	}
	stat, err := dataFile.Stat()
	if err != nil {
		log.WithFields(log.Fields{"meta_file": meta.MetaFilepath, "data_file": meta.DataFilepath}).Warn("error happened during creating of fileblock, removing files")
		os.Remove(meta.DataFilepath)
		os.Remove(meta.MetaFilepath)
		return nil, err
	}
	meta.Size = stat.Size()

	metaFile, err := os.Create(meta.MetaFilepath)
	if err != nil {
		log.WithFields(log.Fields{"meta_file": meta.MetaFilepath, "data_file": meta.DataFilepath}).Warn("error happened during creating of fileblock, removing files")
		os.Remove(meta.DataFilepath)
		os.Remove(meta.MetaFilepath)
		return nil, errors.Join(errors.New("error creating meta file: "), err)
	}
	defer metaFile.Close()
	if err = json.NewEncoder(metaFile).Encode(meta); err != nil {
		log.WithFields(log.Fields{"meta_file": meta.MetaFilepath, "data_file": meta.DataFilepath}).Warn("error happened during creating of fileblock, removing files")
		os.Remove(meta.DataFilepath)
		os.Remove(meta.MetaFilepath)
		return nil, err
	}

	return NewLocalFileblock(f.cfg, meta, f), nil
}

func (f *localJSONFs[T]) MoveToLevel(m *streedb.MetaFile[T]) error {
	return moveToNewLocalLevel(f.cfg, m)
}

func (f *localJSONFs[T]) UpdateMetadata(b streedb.Fileblock[T]) error {
	return updateMetadata(b.Metadata())
}

func (f *localJSONFs[T]) Remove(b streedb.Fileblock[T]) error {
	return remove(b.Metadata())
}

func (f *localJSONFs[T]) OpenAllMetaFiles() (streedb.Levels[T], error) {
	filesystem := streedb.Filesystem[T](f)

	levels := streedb.NewLevels(f.cfg, filesystem)

	initialSearchPath := f.cfg.DbPath

	return levels, metaFilesInDir(f.cfg, filesystem, initialSearchPath, &levels)
}

// newJSONLocalFileblock is used to create new JSON files.
// `entries` must contain the data to be written to the file.
// `level` is the destination level for the filebeock.
func newJSONLocalFileblock[T streedb.Entry](cfg *streedb.Config, entries streedb.Entries[T], level int, fs streedb.Filesystem[T]) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".jsondata").
		Build()
	if err != nil {
		return nil, err
	}

	return NewLocalFileblock(cfg, meta, fs), nil
}
