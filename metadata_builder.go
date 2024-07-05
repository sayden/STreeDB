package streedb

import (
	"path"
	"time"
)

func NewMetadataBuilder[T Entry]() *MetadataBuilder[T] {
	return &MetadataBuilder[T]{MetaFile: MetaFile[T]{Uuid: NewUUID(), CreatedAt: time.Now()}}
}

type MetadataBuilder[T Entry] struct {
	fileExtension  string
	filenamePrefix string
	fullFilepath   string
	rootPath       string

	MetaFile[T]
}

func (b *MetadataBuilder[T]) GetLevel() int {
	return b.Level
}

func (b *MetadataBuilder[T]) WithRootPath(p string) *MetadataBuilder[T] {
	b.rootPath = p
	return b
}

func (b *MetadataBuilder[T]) WithFilepath(p string) *MetadataBuilder[T] {
	b.MetaFilepath = p
	return b
}

func (b *MetadataBuilder[T]) WithFullFilepath(p string) *MetadataBuilder[T] {
	b.MetaFilepath = p
	return b
}

func (b *MetadataBuilder[T]) WithExtension(e string) *MetadataBuilder[T] {
	b.fileExtension = e
	return b
}

func (b *MetadataBuilder[T]) WithEntries(es Entries[T]) *MetadataBuilder[T] {
	var min, max Entry

	if es.Len() > 1 {
		min = es[0]
		max = es[es.Len()-1]
	} else if es.Len() == 1 {
		min = es[0]
		max = es[0]
	}

	b.ItemCount = len(es)
	b.Min = min.(T)
	b.Max = max.(T)

	return b
}

func (b *MetadataBuilder[T]) WithFilename(s string) *MetadataBuilder[T] {
	b.Uuid = s
	return b
}

func (b *MetadataBuilder[T]) WithFilenamePrefix(prefix string) *MetadataBuilder[T] {
	b.filenamePrefix = prefix
	return b
}

func (b *MetadataBuilder[T]) WithLevel(l int) *MetadataBuilder[T] {
	b.Level = l
	return b
}

func (b *MetadataBuilder[T]) Build() (*MetaFile[T], error) {
	if b.Uuid == "" {
		b.Uuid = NewUUID()
	}

	if b.MetaFilepath != "" {
		return &b.MetaFile, nil
	}

	if b.fileExtension == "" {
		if b.filenamePrefix == "" {
			// no extension, no prefix
			b.MetaFilepath = path.Join(b.rootPath, "meta_"+b.Uuid+".json")
			b.DataFilepath = path.Join(b.rootPath, b.Uuid)
		} else {
			// no extension, prefix
			b.MetaFilepath = path.Join(b.rootPath, b.filenamePrefix, b.filenamePrefix+"meta_"+b.Uuid+".json")
			b.DataFilepath = path.Join(b.rootPath, b.filenamePrefix, b.Uuid)
		}
	} else {
		if b.filenamePrefix == "" {
			// with extension, no prefix
			b.MetaFilepath = path.Join(b.rootPath, "meta_"+b.Uuid+".json")
			b.DataFilepath = path.Join(b.rootPath, b.Uuid+b.fileExtension)
		} else {
			// with extension, prefix
			b.MetaFilepath = path.Join(b.rootPath, b.filenamePrefix+"meta_"+b.Uuid+".json")
			b.DataFilepath = path.Join(b.rootPath, b.filenamePrefix, b.Uuid+b.fileExtension)
		}
	}

	return &b.MetaFile, nil
}
