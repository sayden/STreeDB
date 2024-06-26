package main

import (
	"io"

	"github.com/thehivecorporation/log"
)

type BlockWriters struct {
	Uuid string

	DataFilepath string
	MetaFilepath string

	dataFile io.ReadWriteCloser
	metaFile io.ReadWriteCloser
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
