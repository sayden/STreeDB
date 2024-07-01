package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

type localFileblockBuilder[T streedb.Entry] func(cfg *streedb.Config, meta *streedb.MetaFile[T], f streedb.Filesystem[T]) streedb.Fileblock[T]

func NewLocalFileblockParquet[T streedb.Entry](cfg *streedb.Config, meta *streedb.MetaFile[T], f streedb.Filesystem[T]) streedb.Fileblock[T] {
	return &localParquetFileblock[T]{
		MetaFile: *meta,
		fs:       f,
		cfg:      cfg,
	}
}

func NewLocalFileblockJSON[T streedb.Entry](cfg *streedb.Config, meta *streedb.MetaFile[T], f streedb.Filesystem[T]) streedb.Fileblock[T] {
	return &localJSONFileblock[T]{
		MetaFile: *meta,
		fs:       f,
		cfg:      cfg,
	}
}

func metaFilesInDir[T streedb.Entry](cfg *streedb.Config, f streedb.Filesystem[T], folder string, levels *streedb.Levels[T], builder localFileblockBuilder[T]) error {
	files, err := os.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			err2 := metaFilesInDir(cfg, f, path.Join(folder, file.Name()), levels, builder)
			if err2 != nil {
				return err2
			}
		}

		if path.Ext(file.Name()) != ".json" {
			continue
		}

		meta, err := f.Open(path.Join(folder, file.Name()))
		if err != nil {
			return err
		}
		lb := builder(cfg, meta, f)
		(*levels).AppendFile(lb)
	}

	return nil
}

func find[T streedb.Entry](l streedb.Fileblock[T], v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInsideMinMax(l.Metadata().Min, l.Metadata().Max, v) {
		return nil, false, nil
	}

	entries, err := l.Load()
	if err != nil {
		return nil, false, errors.Join(errors.New("error loading block"), err)
	}

	entry, found := entries.Find(v)
	return entry, found, nil
}

func open[T streedb.Entry](p string) (meta *streedb.MetaFile[T], err error) {
	var file *os.File
	if file, err = os.Open(p); err != nil {
		return
	}
	defer file.Close()

	meta = &streedb.MetaFile[T]{
		MetaFilepath: p,
	}

	if err = json.NewDecoder(file).Decode(&meta); err != nil {
		return nil, err
	}

	return meta, nil
}

func remove[T streedb.Entry](m *streedb.MetaFile[T]) error {
	log.Debugf("Removing parquet block data in '%s'", m.DataFilepath)
	if err := os.Remove(m.DataFilepath); err != nil {
		return err
	}

	log.Debugf("Removing parquet block's meta in '%s'", m.MetaFilepath)
	if err := os.Remove(m.MetaFilepath); err != nil {
		return err
	}

	return nil
}

type localFilesystemBuilder[T streedb.Entry] func(c *streedb.Config) streedb.Filesystem[T]

func jSONFsBuilder[T streedb.Entry](c *streedb.Config) streedb.Filesystem[T] {
	return &localJSONFs[T]{cfg: c}
}

func parquetFsBuilder[T streedb.Entry](c *streedb.Config) streedb.Filesystem[T] {
	return &localParquetFs[T]{cfg: c}
}

func initLocal[T streedb.Entry](c *streedb.Config, fsBuilder localFilesystemBuilder[T]) (streedb.Filesystem[T], streedb.Levels[T], error) {
	if !path.IsAbs(c.DbPath) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, nil, err
		}
		c.DbPath = path.Join(cwd, c.DbPath)
	}

	os.MkdirAll(c.DbPath, 0755)

	folders := make([]string, 0, c.MaxLevels+1)

	for i := 0; i < c.MaxLevels; i++ {
		level := path.Join(c.DbPath, fmt.Sprintf("%02d", i))
		folders = append(folders, level)
	}
	folders = append(folders, path.Join(c.DbPath, "wal"))

	for _, folder := range folders {
		os.MkdirAll(folder, 0755)
	}

	fs := fsBuilder(c)
	meta, err := fs.OpenAllMetaFiles()
	if err != nil {
		return nil, nil, err
	}

	return fs, meta, nil
}
