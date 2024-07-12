package fslocal

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path"

	db "github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

func initLocal[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, level int) (db.Filesystem[O, E], error) {
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

func open[O cmp.Ordered, T db.Entry[O]](cfg *db.Config, f db.Filesystem[O, T], p string, listeners ...db.FileblockListener[O, T]) (*db.Fileblock[O, T], error) {
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

func metaFilesInDir[O cmp.Ordered, T db.Entry[O]](cfg *db.Config, folder string, f db.Filesystem[O, T], listeners ...db.FileblockListener[O, T]) error {
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

		_, err = open(cfg, f, path.Join(folder, file.Name()), listeners...)
		if err != nil {
			return err
		}

	}

	return nil
}

func remove[O cmp.Ordered, E db.Entry[O]](fb *db.Fileblock[O, E], ls ...db.FileblockListener[O, E]) error {
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

func updateMetadata[O cmp.Ordered, E db.Entry[O]](meta *db.MetaFile[O]) error {
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
