package fslocal

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	db "github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

type localFilesystemBuilder[T db.Entry] func(c *db.Config, rootPath string) db.Filesystem[T]

func jSONFsBuilder[T db.Entry](c *db.Config, rootPath string) db.Filesystem[T] {
	return &localJSONFs[T]{cfg: c, rootPath: rootPath}
}

func parquetFsBuilder[T db.Entry](c *db.Config, rootPath string) db.Filesystem[T] {
	return &localParquetFs[T]{cfg: c, rootPath: rootPath}
}

func initLocal[T db.Entry](c *db.Config, level int, fsBuilder localFilesystemBuilder[T]) (db.Filesystem[T], error) {
	rootPath := path.Join(c.DbPath, fmt.Sprintf("%02d", level))
	if !path.IsAbs(rootPath) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		rootPath = path.Join(cwd, c.DbPath)
	}

	os.MkdirAll(rootPath, 0755)

	fs := fsBuilder(c, rootPath)

	return fs, nil
}

func open[T db.Entry](p string) (meta *db.MetaFile[T], err error) {
	var file *os.File
	if file, err = os.Open(p); err != nil {
		return
	}
	defer file.Close()

	meta = &db.MetaFile[T]{
		MetaFilepath: p,
	}

	if err = json.NewDecoder(file).Decode(&meta); err != nil {
		return nil, err
	}

	return meta, nil
}

func metaFilesInDir[T db.Entry](cfg *db.Config, folder string, f db.Filesystem[T], level db.Level[T]) error {
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

		meta, err := level.Open(path.Join(folder, file.Name()))
		if err != nil {
			return err
		}

		lb := NewLocalFileblock(cfg, meta, f)
		level.AppendFileblock(lb)
	}

	return nil
}

func remove[T db.Entry](m *db.MetaFile[T]) error {
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

func updateMetadata[T db.Entry](meta *db.MetaFile[T]) error {
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
