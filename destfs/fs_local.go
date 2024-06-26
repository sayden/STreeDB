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

var (
	default_DB_PATH = "/tmp/test"
)

type fs[T streedb.Entry] struct {
	path string
}

func InitStartup[T streedb.Entry](initialPath string) (*fs[T], streedb.Levels[T], error) {
	default_DB_PATH = initialPath

	if !path.IsAbs(initialPath) {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, nil, err
		}
		initialPath = path.Join(cwd, initialPath)
	}

	os.MkdirAll(initialPath, 0755)

	folders := make([]string, 0, streedb.MAX_LEVELS+1)

	for i := 0; i < streedb.MAX_LEVELS; i++ {
		level := path.Join(initialPath, fmt.Sprintf("%02d", i))
		folders = append(folders, level)
	}
	folders = append(folders, path.Join(initialPath, "wal"))

	for _, folder := range folders {
		os.MkdirAll(folder, 0755)
	}

	fs := &fs[T]{path: initialPath}
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
		meta, err := fileformat.NewEmptyParquetBlock(min, max, path.Join(folder, file.Name()))
		if err != nil {
			return err
		}

		// read metadata
		if err = json.NewDecoder(meta.GetMeta()).Decode(&meta.Block); err != nil {
			return errors.Join(errors.New("error decoding metadata file: "), err)
		}

		(*levels).AppendBlock(meta)
	}

	return nil
}
