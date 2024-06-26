package streedb

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/thehivecorporation/log"
)

type BlockWriters struct {
	Uuid string

	DataFilepath string
	MetaFilepath string

	dataFile io.ReadWriteCloser
	metaFile io.ReadWriteCloser
}

func NewBlockWriter(filename string, l int) (bfs *BlockWriters, err error) {
	ext := path.Ext(filename)
	fNoExtension := strings.ReplaceAll(filename, ext, "")

	bfs = &BlockWriters{
		MetaFilepath: path.Join(DEFAULT_DB_PATH, fmt.Sprintf("%02d", l), "meta_"+fNoExtension+".json"),
		DataFilepath: path.Join(DEFAULT_DB_PATH, fmt.Sprintf("%02d", l), filename),
	}

	if bfs.dataFile, err = os.Create(bfs.DataFilepath); err != nil {
		return nil, err
	}

	if bfs.metaFile, err = os.Create(bfs.MetaFilepath); err != nil {
		return nil, err
	}

	return
}

func (b *BlockWriters) Close() {
	if b.dataFile != nil {
		log.Debugf("Closing data file %s", b.DataFilepath)
		b.dataFile.Close()
	}

	if b.metaFile != nil {
		log.Debugf("Closing meta file %s", b.DataFilepath)
		b.metaFile.Close()
	}
}

func (b *BlockWriters) GetData() io.ReadWriteCloser {
	return b.dataFile
}

func (b *BlockWriters) SetData(m io.ReadWriteCloser) {
	b.dataFile = m
}

func (b *BlockWriters) GetMeta() io.ReadWriteCloser {
	return b.metaFile
}

func (b *BlockWriters) SetMeta(m io.ReadWriteCloser) {
	b.metaFile = m
}
