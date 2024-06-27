package streedb

import (
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/thehivecorporation/log"
)

type FileBlockRW struct {
	Uuid string

	DataFilepath string
	MetaFilepath string

	dataFile io.ReadWriteCloser
	metaFile io.ReadWriteCloser
}

func NewBlockWriter[T Entry](filename string, l int, fs DestinationFs[T]) (bfs *FileBlockRW, err error) {
	ext := path.Ext(filename)
	fNoExtension := strings.ReplaceAll(filename, ext, "")

	bfs = &FileBlockRW{
		MetaFilepath: path.Join(DEFAULT_DB_PATH, fmt.Sprintf("%02d", l), "meta_"+fNoExtension+".json"),
		DataFilepath: path.Join(DEFAULT_DB_PATH, fmt.Sprintf("%02d", l), filename),
	}

	if bfs.dataFile, err = fs.Create(bfs.DataFilepath); err != nil {
		return nil, err
	}

	if bfs.metaFile, err = fs.Create(bfs.MetaFilepath); err != nil {
		return nil, err
	}

	return
}

func (b *FileBlockRW) Close() {
	if b.dataFile != nil {
		log.Debugf("Closing data file %s", b.DataFilepath)
		b.dataFile.Close()
	}

	if b.metaFile != nil {
		log.Debugf("Closing meta file %s", b.MetaFilepath)
		b.metaFile.Close()
	}
}

func (b *FileBlockRW) GetData() io.ReadWriteCloser {
	return b.dataFile
}

func (b *FileBlockRW) SetData(m io.ReadWriteCloser) {
	b.dataFile = m
}

func (b *FileBlockRW) GetMeta() io.ReadWriteCloser {
	return b.metaFile
}

func (b *FileBlockRW) SetMeta(m io.ReadWriteCloser) {
	b.metaFile = m
}
