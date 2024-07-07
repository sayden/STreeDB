package fslocal

import (
	"encoding/json"
	"errors"
	"os"

	db "github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// InitParquetLocal initializes a local filesystem destination. Writes the folder structure if required
// and then read the medatada files that are already there.
func InitParquetLocal[T db.Entry](c *db.Config, level int) (db.Filesystem[T], error) {
	return initLocal[T](c, level, parquetFsBuilder)
}

type localParquetFs[T db.Entry] struct {
	cfg      *db.Config
	rootPath string
}

func (f *localParquetFs[T]) UpdateMetadata(b *db.Fileblock[T]) error {
	return updateMetadata(b.Metadata())
}

// Load the parquet file using the data stored in the metadata file
func (f *localParquetFs[T]) Load(b *db.Fileblock[T]) (db.Entries[T], error) {
	pf, err := local.NewLocalFileReader(b.Metadata().DataFilepath)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	pr, err := reader.NewParquetReader(pf, new(T), db.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make(db.Entries[T], numRows)
	err = pr.Read(&entries)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func (f *localParquetFs[T]) Create(cfg *db.Config, entries db.Entries[T], meta *db.MetaFile[T], ls []db.FileblockListener[T]) (*db.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	dataFile, err := os.Create(meta.DataFilepath)
	if err != nil {
		return nil, errors.Join(errors.New("error creating data file: "), err)
	}
	defer dataFile.Close()

	parquetWriter, err := writer.NewParquetWriterFromWriter(dataFile, new(T), db.PARQUET_NUMBER_OF_THREADS)
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

	block := db.NewFileblock(f.cfg, meta, f)
	for _, listener := range ls {
		listener.OnNewFileblock(block)
	}

	return block, nil
}

func (f *localParquetFs[T]) Remove(b *db.Fileblock[T], ls []db.FileblockListener[T]) error {
	return remove(b, ls...)
}

func (f *localParquetFs[T]) OpenMetaFilesInLevel(listeners []db.FileblockListener[T]) error {
	return metaFilesInDir(f.cfg, f.rootPath, f, listeners...)
}

func (f *localParquetFs[T]) FillMetadataBuilder(meta *db.MetadataBuilder[T]) *db.MetadataBuilder[T] {
	return meta.WithRootPath(f.rootPath).WithExtension(".parquet")
}
