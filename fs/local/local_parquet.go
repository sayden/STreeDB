package fslocal

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	db "github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// InitParquetLocal initializes a local filesystem destination. Writes the folder structure if required
// and then read the medatada files that are already there.
func InitParquetLocal[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, level int) (db.Filesystem[O], error) {
	rootPath := path.Join(cfg.DbPath, fmt.Sprintf("%02d", level))
	if !path.IsAbs(rootPath) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		rootPath = path.Join(cwd, cfg.DbPath)
	}

	os.MkdirAll(rootPath, 0755)

	fs := &localParquetFs[O, E]{cfg: cfg, rootPath: rootPath}

	return fs, nil
}

type localParquetFs[O cmp.Ordered, E db.Entry[O]] struct {
	cfg      *db.Config
	rootPath string
}

func (f *localParquetFs[O, _]) UpdateMetadata(b *db.Fileblock[O]) error {
	meta := b.Metadata()

	file, err := os.Create(meta.MetaFilepath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err = file.Truncate(0); err != nil {
		return err
	}

	if err = json.NewEncoder(file).Encode(meta); err != nil {
		return err
	}

	return file.Sync()
}

// Load the parquet file using the data stored in the metadata file
func (f *localParquetFs[O, E]) Load(b *db.Fileblock[O]) (db.EntriesMap[O], error) {
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

func (f *localParquetFs[O, E]) Create(cfg *db.Config, es db.EntriesMap[O], builder *db.MetadataBuilder[O], ls []db.FileblockListener[O]) (*db.Fileblock[O], error) {
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

func (f *localParquetFs[O, _]) Remove(fb *db.Fileblock[O], ls []db.FileblockListener[O]) error {
	m := fb.Metadata()

	log.Debugf("Removing parquet block data in '%s'", m.DataFilepath)
	if err := os.Remove(m.DataFilepath); err != nil {
		return err
	}

	log.Debugf("Removing parquet block's meta in '%s'", m.MetaFilepath)
	if err := os.Remove(m.MetaFilepath); err != nil {
		return err
	}

	for _, listener := range ls {
		listener.OnFileblockRemoved(fb)
	}

	return nil
}

func (f *localParquetFs[O, _]) OpenMetaFilesInLevel(listeners []db.FileblockListener[O]) error {
	folder := f.rootPath
	files, err := os.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			panic("folder not expected")
		}

		if path.Ext(file.Name()) != ".json" {
			continue
		}

		if _, err = open(f.cfg, f, path.Join(folder, file.Name()), listeners...); err != nil {
			return err
		}

	}

	return nil
}

func (f *localParquetFs[O, _]) FillMetadataBuilder(meta *db.MetadataBuilder[O]) *db.MetadataBuilder[O] {
	return meta.WithRootPath(f.rootPath).WithExtension(".parquet")
}

func open[O cmp.Ordered](cfg *db.Config, f db.Filesystem[O], p string, listeners ...db.FileblockListener[O]) (*db.Fileblock[O], error) {
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	meta := &db.MetaFile[O]{MetaFilepath: p}

	if err = json.NewDecoder(file).Decode(&meta); err != nil {
		return nil, err
	}

	block := db.NewFileblock(cfg, meta, f)
	for _, listener := range listeners {
		listener.OnFileblockCreated(block)
	}

	return block, nil
}
