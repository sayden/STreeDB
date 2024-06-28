package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// InitParquetLocal initializes a local filesystem destination. Writes the folder structure if required
// and then read the medatada files that are already there.
func InitParquetLocal[T streedb.Entry](c *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	return initLocal[T](c, parquetFsBuilder)
}

type localParquetFs[T streedb.Entry] struct {
	cfg *streedb.Config
}

func (f *localParquetFs[T]) Open(p string) (meta *streedb.MetaFile[T], err error) {
	return open[T](p)
}

// Load the parquet file using the data stored in the metadata file
func (f *localParquetFs[T]) Load(m *streedb.MetaFile[T]) (streedb.Entries[T], error) {
	pf, err := local.NewLocalFileReader(m.DataFilepath)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	pr, err := reader.NewParquetReader(pf, new(T), streedb.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make(streedb.Entries[T], numRows)
	err = pr.Read(&entries)
	if err != nil {
		return nil, err
	}

	log.Debugf("Reading parquet file %s with %d rows", m.DataFilepath, numRows)

	return entries, nil
}

func (f *localParquetFs[T]) Create(entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](f.cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".parquet").
		WithFilenamePrefix(fmt.Sprintf("%02d/", level)).
		Build()
	if err != nil {
		return nil, err
	}

	dataFile, err := os.Create(meta.DataFilepath)
	if err != nil {
		return nil, errors.Join(errors.New("error creating data file: "), err)
	}
	defer dataFile.Close()

	parquetWriter, err := writer.NewParquetWriterFromWriter(dataFile, new(T), streedb.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		parquetWriter.Write(entry)
	}

	if err = parquetWriter.WriteStop(); err != nil {
		panic(err)
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

	return &localParquetFileblock[T]{MetaFile: *meta, fs: f}, nil
}

func (f *localParquetFs[T]) Remove(m *streedb.MetaFile[T]) error {
	return remove(m)
}

func (f *localParquetFs[T]) OpenAllMetaFiles() (streedb.Levels[T], error) {
	levels := streedb.NewLevels[T](f.cfg)
	return levels, metaFilesInDir(f, f.cfg.DbPath, &levels, usingParquet)
}

func newParquetFileblock[T streedb.Entry](entries streedb.Entries[T], cfg *streedb.Config, level int, fs streedb.Filesystem[T]) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".parquet").
		WithFilenamePrefix(fmt.Sprintf("%02d/", level)).
		Build()
	if err != nil {
		return nil, err
	}

	return &localParquetFileblock[T]{
		MetaFile: *meta,
		fs:       fs,
		cfg:      cfg,
	}, nil
}

type localParquetFileblock[T streedb.Entry] struct {
	streedb.MetaFile[T]

	cfg *streedb.Config
	fs  streedb.Filesystem[T]
}

func (l *localParquetFileblock[T]) Load() (streedb.Entries[T], error) {
	return l.fs.Load(&l.MetaFile)
}

func (l *localParquetFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	return find(l, v)
}

// Merge the entries from this block with the entries of `a` and return the new block
func (l *localParquetFileblock[T]) Merge(a streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	dest, err := merge(l, a)
	if err != nil {
		return nil, err
	}

	// TODO: optimistic creation of new block
	return newParquetFileblock(dest, l.cfg, l.Level+1, l.fs)
}

func (l *localParquetFileblock[T]) Remove() error {
	return remove(&l.MetaFile)
}

func (l *localParquetFileblock[T]) Metadata() *streedb.MetaFile[T] {
	return &l.MetaFile
}

func (l *localParquetFileblock[T]) Close() error {
	//noop
	return nil
}
