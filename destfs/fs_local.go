package destfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/sayden/streedb"
	"github.com/sayden/streedb/fileformat"
)

type fs[T streedb.Entry] struct {
	path string
}

func InitStartup[T streedb.Entry](initialPath string) (streedb.DestinationFs[T], streedb.Levels[T], error) {
	streedb.DEFAULT_DB_PATH = initialPath

	if !path.IsAbs(streedb.DEFAULT_DB_PATH) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, nil, err
		}
		streedb.DEFAULT_DB_PATH = path.Join(cwd, streedb.DEFAULT_DB_PATH)
	}

	os.MkdirAll(streedb.DEFAULT_DB_PATH, 0755)

	folders := make([]string, 0, streedb.MAX_LEVELS+1)

	for i := 0; i < streedb.MAX_LEVELS; i++ {
		level := path.Join(streedb.DEFAULT_DB_PATH, fmt.Sprintf("%02d", i))
		folders = append(folders, level)
	}
	folders = append(folders, path.Join(streedb.DEFAULT_DB_PATH, "wal"))

	for _, folder := range folders {
		os.MkdirAll(folder, 0755)
	}

	fs := &fs[T]{path: streedb.DEFAULT_DB_PATH}
	meta, err := fs.MetaFiles()
	if err != nil {
		return nil, nil, err
	}

	return fs, meta, nil
}

func (f *fs[T]) MetaFiles() (streedb.Levels[T], error) {
	levels := streedb.NewLevels[T](streedb.MAX_LEVELS)
	return levels, metaFilesInDir(f, f.path, &levels)
}

func metaFilesInDir[T streedb.Entry](f *fs[T], folder string, levels *streedb.Levels[T]) error {
	files, err := os.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			err2 := metaFilesInDir(f, path.Join(f.path, file.Name()), levels)
			if err2 != nil {
				return err2
			}
		}

		if path.Ext(file.Name()) != ".json" {
			continue
		}

		min := new(T)
		max := new(T)
		meta, err := fileformat.NewEmptyFormat(min, max, path.Join(folder, file.Name()))
		if err != nil {
			return err
		}

		// read metadata
		if err = json.NewDecoder(meta.GetBlock().GetMeta()).Decode(meta.GetBlock()); err != nil {
			return errors.Join(errors.New("error decoding metadata file: "), err)
		}

		(*levels).AppendBlock(meta)
	}

	return nil
}
