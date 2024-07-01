package fs

import (
	"encoding/json"
	"errors"
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

func (f *localParquetFs[T]) UpdateMetadata(b streedb.Fileblock[T]) error {
	return updateMetadata(b.Metadata())
}

// Load the parquet file using the data stored in the metadata file
func (f *localParquetFs[T]) Load(b streedb.Fileblock[T]) (streedb.Entries[T], error) {
	pf, err := local.NewLocalFileReader(b.Metadata().DataFilepath)
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

	log.Debugf("Reading parquet file %s with %d rows", b.Metadata().DataFilepath, numRows)

	return entries, nil
}

func (f *localParquetFs[T]) Merge(a, b streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	newEntries, err := Merge(a, b)
	if err != nil {
		return nil, err
	}
	return f.Create(f.cfg, newEntries, a.Metadata().Level)
}

func (f *localParquetFs[T]) Create(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](f.cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".parquet").
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

	return NewLocalFileblockParquet(f.cfg, meta, f), nil
}

func (f *localParquetFs[T]) Remove(b streedb.Fileblock[T]) error {
	return remove(b.Metadata())
}

func (f *localParquetFs[T]) OpenAllMetaFiles() (streedb.Levels[T], error) {
	filesystem := streedb.Filesystem[T](f)

	levels := streedb.NewLevels(f.cfg, filesystem)

	initialSearchPath := f.cfg.DbPath

	return levels, metaFilesInDir(f.cfg, filesystem, initialSearchPath, &levels, NewLocalFileblockJSON)
}

func newParquetFileblock[T streedb.Entry](entries streedb.Entries[T], cfg *streedb.Config, level int, fs streedb.Filesystem[T]) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T](cfg.DbPath).
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".parquet").
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

func (l *localParquetFileblock[T]) UUID() string {
	return l.Uuid
}

func (l *localParquetFileblock[T]) Load() (streedb.Entries[T], error) {
	return l.fs.Load(l)
}

func (l *localParquetFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	return find(l, v)
}

func (l *localParquetFileblock[T]) Metadata() *streedb.MetaFile[T] {
	return &l.MetaFile
}

func (l *localParquetFileblock[T]) Close() error {
	//noop
	return nil
}
