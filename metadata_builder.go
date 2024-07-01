package streedb

import (
	"path"

	"github.com/thehivecorporation/log"
)

type MetadataBuilder[T Entry] interface {
	WithExtension(string) MetadataBuilder[T]
	WithEntries(Entries[T]) MetadataBuilder[T]
	WithFilenamePrefix(string) MetadataBuilder[T]
	WithLevel(int) MetadataBuilder[T]
	WithFilename(string) MetadataBuilder[T]
	WithFullFilepath(string) MetadataBuilder[T]
	Build() (*MetaFile[T], error)
}

type metadataBuilder[T Entry] struct {
	fileExtension  string
	filenamePrefix string
	fullFilepath   string
	rootPath       string

	metaFile MetaFile[T]
}

func (b *metadataBuilder[T]) WithFilepath(p string) MetadataBuilder[T] {
	b.metaFile.MetaFilepath = p
	return b
}

func (b *metadataBuilder[T]) WithFullFilepath(p string) MetadataBuilder[T] {
	b.metaFile.MetaFilepath = p
	return b
}

func (b *metadataBuilder[T]) WithExtension(e string) MetadataBuilder[T] {
	b.fileExtension = e
	return b
}

func (b *metadataBuilder[T]) WithEntries(es Entries[T]) MetadataBuilder[T] {
	var min, max Entry

	if es.Len() > 1 {
		min = es[0]
		max = es[es.Len()-1]
	} else if es.Len() == 1 {
		min = es[0]
		max = es[0]
	}

	b.metaFile.ItemCount = len(es)
	b.metaFile.Min = min.(T)
	b.metaFile.Max = max.(T)

	return b
}

func (b *metadataBuilder[T]) WithFilename(s string) MetadataBuilder[T] {
	if b.metaFile.Uuid != "" {
		log.WithFields(log.Fields{"old_uuid": b.metaFile.Uuid, "new_uuid": s}).Warn("Overwriting UUID with filename")
	}
	b.metaFile.Uuid = s
	return b
}

func (b *metadataBuilder[T]) WithFilenamePrefix(prefix string) MetadataBuilder[T] {
	b.filenamePrefix = prefix
	return b
}

func (b *metadataBuilder[T]) WithLevel(l int) MetadataBuilder[T] {
	b.metaFile.Level = l
	return b
}

func (b *metadataBuilder[T]) Build() (*MetaFile[T], error) {
	if b.metaFile.Uuid == "" {
		b.metaFile.Uuid = NewUUID()
	}

	if b.metaFile.MetaFilepath != "" {
		return &b.metaFile, nil
	}

	if b.fileExtension == "" {
		if b.filenamePrefix == "" {
			// no extension
			b.metaFile.MetaFilepath = path.Join(b.rootPath, "meta_"+b.metaFile.Uuid+".json")
			b.metaFile.DataFilepath = path.Join(b.rootPath, b.metaFile.Uuid)
		} else {
			// no extension
			b.metaFile.MetaFilepath = path.Join(b.rootPath, b.filenamePrefix, b.filenamePrefix+"meta_"+b.metaFile.Uuid+".json")
			b.metaFile.DataFilepath = path.Join(b.rootPath, b.filenamePrefix+b.metaFile.Uuid)
		}
	} else {
		if b.filenamePrefix == "" {
			// with extension
			b.metaFile.MetaFilepath = path.Join(b.rootPath, "meta_"+b.metaFile.Uuid+".json")
			b.metaFile.DataFilepath = path.Join(b.rootPath, b.metaFile.Uuid+b.fileExtension)
		} else {
			// with extension
			b.metaFile.MetaFilepath = path.Join(b.rootPath, b.filenamePrefix+"meta_"+b.metaFile.Uuid+".json")
			b.metaFile.DataFilepath = path.Join(b.rootPath, b.filenamePrefix+b.metaFile.Uuid+b.fileExtension)
		}
	}

	return &b.metaFile, nil
}
