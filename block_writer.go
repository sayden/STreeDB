package streedb

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/thehivecorporation/log"
)

type BlockWriters struct {
	Uuid string

	DataFilepath string
	MetaFilepath string

	dataFile io.ReadWriteCloser
	metaFile io.ReadWriteCloser
}

func NewBlockWriter(defaultFolder string, l int) (bfs *BlockWriters, err error) {
	bfs = &BlockWriters{}

	bfs.Uuid = newUUID()

	bfs.DataFilepath = path.Join(defaultFolder, fmt.Sprintf("%02d", l), bfs.Uuid)
	dataFile, err := os.Create(bfs.DataFilepath)
	if err != nil {
		return nil, err
	}
	bfs.dataFile = dataFile

	bfs.MetaFilepath = path.Join(defaultFolder, fmt.Sprintf("%02d", l), "meta_"+bfs.Uuid+".json")
	bfs.metaFile, err = os.Create(bfs.MetaFilepath)
	if err != nil {
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
