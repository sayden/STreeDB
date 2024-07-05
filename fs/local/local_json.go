package fslocal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	db "github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

func InitJSONLocal[T db.Entry](cfg *db.Config, level int) (db.Filesystem[T], error) {
	return initLocal[T](cfg, level, jSONFsBuilder)
}

type localJSONFs[T db.Entry] struct {
	rootPath string

	cfg *db.Config
}

func (f *localJSONFs[T]) Open(p string) (meta *db.MetaFile[T], err error) {
	return open[T](p)
}

func (f *localJSONFs[T]) Load(b db.Fileblock[T]) (db.Entries[T], error) {
	file, err := os.Open(b.Metadata().DataFilepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries := make(db.Entries[T], 0)
	if err = json.NewDecoder(file).Decode(&entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func (f *localJSONFs[T]) Create(cfg *db.Config, entries db.Entries[T], meta *db.MetaFile[T]) (db.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
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

	return db.NewFileblock(f.cfg, meta, f), nil
}

func (f *localJSONFs[T]) UpdateMetadata(b db.Fileblock[T]) error {
	return updateMetadata(b.Metadata())
}

func (f *localJSONFs[T]) Remove(b db.Fileblock[T]) error {
	return remove(b.Metadata())
}

func (f *localJSONFs[T]) OpenMetaFilesInLevel(level db.Level[T]) error {
	return metaFilesInDir(f.cfg, f.rootPath, f, level)
}

func (f *localJSONFs[T]) OpenAllMetaFiles() (db.Levels[T], error) {
	// filesystem := db.Filesystem[T](f)
	// levels := db.NewLevels(f.cfg, filesystem)
	// return levels, metaFilesInFolders(f.cfg, filesystem, f.rootPath, levels)
	panic("not implemented")
}

func (f *localJSONFs[T]) FillMetadataBuilder(meta *db.MetadataBuilder[T]) *db.MetadataBuilder[T] {
	return meta.WithRootPath(f.rootPath).WithExtension(".jsondata")
}
