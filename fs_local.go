package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
)

type Fs[T Entry] struct {
	defaultFolder string
}

func InitStartup[T Entry](def string) (*Fs[T], Levels[T], error) {
	DEFAULT_DB_PATH = def

	defaultFolder := def

	if !path.IsAbs(defaultFolder) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, nil, err
		}
		defaultFolder = path.Join(cwd, def)
	}

	os.MkdirAll(defaultFolder, 0755)

	folders := make([]string, 0, MAX_LEVELS+1)

	for i := 0; i < MAX_LEVELS; i++ {
		level := path.Join(defaultFolder, fmt.Sprintf("%02d", i))
		folders = append(folders, level)
	}
	folders = append(folders, path.Join(defaultFolder, "wal"))

	for _, folder := range folders {
		os.MkdirAll(folder, 0755)
	}

	fs := &Fs[T]{defaultFolder: defaultFolder}
	meta, err := fs.MetaFiles()
	if err != nil {
		return nil, nil, err
	}

	return fs, meta, nil
}

func (f *Fs[T]) MetaFiles() (Levels[T], error) {
	levels := NewLevels[T]()
	return levels, metaFilesInDir(f, f.defaultFolder, &levels)
}

func metaFilesInDir[T Entry](f *Fs[T], folder string, levels *Levels[T]) error {
	files, err := os.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			err2 := metaFilesInDir[T](f, path.Join(f.defaultFolder, file.Name()), levels)
			if err2 != nil {
				return err2
			}
		}

		if path.Ext(file.Name()) != ".json" {
			continue
		}

		min := new(T)
		max := new(T)
		meta := &parquetBlock[T]{
			Block: Block[T]{
				BlockWriters: &BlockWriters{
					MetaFilepath: path.Join(folder, file.Name()),
				},
				MinVal: *min,
				MaxVal: *max,
			},
		}
		meta.metaFile, err = os.Open(meta.MetaFilepath)
		if err != nil {
			return err
		}

		// read metadata
		if err = json.NewDecoder(meta.metaFile).Decode(&meta.Block); err != nil {
			return errors.Join(errors.New("error decoding metadata file: "), err)
		}

		levels.AppendBlock(meta)
	}

	return nil
}
