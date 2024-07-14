package fslocal

import (
	"cmp"
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
func InitParquetLocal[O cmp.Ordered, E db.Entry[O]](c *db.Config, level int) (db.Filesystem[O, E], error) {
	return initLocal[O, E](c, level)
}

type localParquetFs[O cmp.Ordered, E db.Entry[O]] struct {
	cfg      *db.Config
	rootPath string
}

func (f *localParquetFs[O, E]) UpdateMetadata(b *db.Fileblock[O, E]) error {
	return updateMetadata[O, E](b.Metadata())
}

// Load the parquet file using the data stored in the metadata file
func (f *localParquetFs[O, E]) Load(b *db.Fileblock[O, E]) (db.EntriesMap[O, E], error) {
	pf, err := local.NewLocalFileReader(b.DataFilepath)
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	pr, err := reader.NewParquetReader(pf, *new(E), db.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make([]E, numRows)
	if err = pr.Read(&entries); err != nil {
		return nil, err
	}
	pr.ReadStop()

	return db.NewSliceToMapWithMetadata(entries, &b.MetaFile), nil
}

func (f *localParquetFs[O, E]) Create(cfg *db.Config, es db.EntriesMap[O, E], builder *db.MetadataBuilder[O], ls []db.FileblockListener[O, E]) (*db.Fileblock[O, E], error) {
	if es.SecondaryIndicesLen() == 0 {
		return nil, errors.New("empty data")
	}

	builder = f.FillMetadataBuilder(builder)
	meta, err := builder.Build()
	if err != nil {
		return nil, errors.Join(errors.New("error building metadata"), err)
	}

	dataFile, err := os.Create(meta.DataFilepath)
	if err != nil {
		return nil, errors.Join(errors.New("error creating data file: "), err)
	}
	defer dataFile.Close()

	parquetWriter, err := writer.NewParquetWriterFromWriter(dataFile, *new(E), db.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		return nil, errors.Join(errors.New("error creating parquet writer: "), err)
	}

	sIdx := es.SecondaryIndices()
	for _, sidx := range sIdx {
		parquetWriter.Write(es.Get(sidx))
	}

	if err = parquetWriter.WriteStop(); err != nil {
		return nil, errors.Join(errors.New("error stopping parquet writer: "), err)
	}

	if err = parquetWriter.Flush(true); err != nil {
		return nil, errors.Join(errors.New("error flushing parquet writer: "), err)
	}

	stat, err := dataFile.Stat()
	if err != nil && len(parquetWriter.Footer.RowGroups) > 0 {
		meta.Size = parquetWriter.Footer.RowGroups[0].TotalByteSize
	} else {
		meta.Size = stat.Size()
	}

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
		listener.OnFileblockCreated(block)
	}

	return block, nil
}

func (f *localParquetFs[O, T]) Remove(b *db.Fileblock[O, T], ls []db.FileblockListener[O, T]) error {
	return remove(b, ls...)
}

func (f *localParquetFs[O, T]) OpenMetaFilesInLevel(listeners []db.FileblockListener[O, T]) error {
	return metaFilesInDir(f.cfg, f.rootPath, f, listeners...)
}

func (f *localParquetFs[O, T]) FillMetadataBuilder(meta *db.MetadataBuilder[O]) *db.MetadataBuilder[O] {
	return meta.WithRootPath(f.rootPath).WithExtension(".parquet")
}
