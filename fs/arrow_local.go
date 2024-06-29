package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

// InitArrowLocal initializes a local filesystem destination. Writes the folder structure if required
// and then read the medatada files that are already there.
func InitArrowLocal[T streedb.Entry](cfg *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	return initLocal[T](cfg, jSONFsBuilder)
}

type localArrowFs[T streedb.Entry] struct {
	cfg *streedb.Config
}

func (f *localArrowFs[T]) Open(p string) (meta *streedb.MetaFile[T], err error) {
	return open[T](p)
}

func (f *localArrowFs[T]) Load(m *streedb.MetaFile[T]) (streedb.Entries[T], error) {
	file, err := os.Open(m.DataFilepath)
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

func (f *localArrowFs[T]) Create(entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](f.cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".arrow").
		WithFilenamePrefix(fmt.Sprintf("%02d/", level)).
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

	return &localArrowFileblock[T]{MetaFile: *meta, fs: f}, nil
}

func (f *localArrowFs[T]) Remove(m *streedb.MetaFile[T]) error {
	return remove(m)
}

func (f *localArrowFs[T]) OpenAllMetaFiles() (streedb.Levels[T], error) {
	levels := streedb.NewLevels[T](f.cfg)
	return levels, metaFilesInDir(f, f.cfg.DbPath, &levels, usingJSON)
}

// newArrowLocalFileblock is used to create new JSON files.
// `entries` must contain the data to be written to the file.
// `level` is the destination level for the filebeock.
func newArrowLocalFileblock[T streedb.Entry](entries streedb.Entries[T], cfg *streedb.Config, level int, fs streedb.Filesystem[T]) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".arrow").
		WithFilenamePrefix(fmt.Sprintf("%02d/", level)).
		Build()
	if err != nil {
		return nil, err
	}

	return &localArrowFileblock[T]{
		MetaFile: *meta,
		fs:       fs,
	}, nil
}

// localArrowFileblock works using plain JSON files to store data (and metadata).
type localArrowFileblock[T streedb.Entry] struct {
	cfg *streedb.Config
	streedb.MetaFile[T]
	path string
	fs   streedb.Filesystem[T]
}

func (l *localArrowFileblock[T]) Load() (streedb.Entries[T], error) {
	return l.fs.Load(&l.MetaFile)
}

func (l *localArrowFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	return find(l, v)
}

// Merge the entries from this block with the entries of `a` and return the new block
func (l *localArrowFileblock[T]) Merge(a streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	dest, err := merge(l, a)
	if err != nil {
		return nil, err
	}

	// TODO: optimistic creation of new block
	return newArrowLocalFileblock(dest, l.cfg, l.Level+1, l.fs)
}

func (l *localArrowFileblock[T]) Remove() error {
	return l.fs.Remove(&l.MetaFile)
}

func (l *localArrowFileblock[T]) Metadata() *streedb.MetaFile[T] {
	return &l.MetaFile
}

func (l *localArrowFileblock[T]) Close() error {
	//noop
	return nil
}
