package streedb

import (
	"cmp"
	"errors"
	"path"
	"time"
)

func NewMetadataBuilder[O cmp.Ordered](cfg *Config) *MetadataBuilder[O] {
	return &MetadataBuilder[O]{
		cfg:      cfg,
		MetaFile: MetaFile[O]{Uuid: NewUUID(), CreatedAt: time.Now()}}
}

type MetadataBuilder[O cmp.Ordered] struct {
	cfg            *Config
	fileExtension  string
	filenamePrefix string
	fullFilepath   string
	rootPath       string

	MetaFile[O]
}

func (b *MetadataBuilder[O]) GetLevel() int {
	return b.Level
}

func (b *MetadataBuilder[O]) WithItemCount(i int) *MetadataBuilder[O] {
	b.ItemCount = i
	return b
}

func (b *MetadataBuilder[O]) WithRootPath(p string) *MetadataBuilder[O] {
	b.rootPath = p
	return b
}

func (b *MetadataBuilder[O]) WithSize(sizeBytes int64) *MetadataBuilder[O] {
	b.Size = sizeBytes
	return b
}

func (b *MetadataBuilder[O]) WithCreatedAt(t time.Time) *MetadataBuilder[O] {
	b.CreatedAt = t
	return b
}

func (b *MetadataBuilder[O]) WithPrimaryIndex(p string) *MetadataBuilder[O] {
	b.PrimaryIdx = p
	return b
}

func (b *MetadataBuilder[O]) WithFilepath(p string) *MetadataBuilder[O] {
	b.MetaFilepath = p
	return b
}

func (b *MetadataBuilder[O]) WithFullFilepath(p string) *MetadataBuilder[O] {
	b.MetaFilepath = p
	return b
}

func (b *MetadataBuilder[O]) WithExtension(e string) *MetadataBuilder[O] {
	b.fileExtension = e

	if b.filenamePrefix == "" {
		b.MetaFilepath = path.Join(b.rootPath, "meta_"+b.Uuid+".json")
		b.DataFilepath = path.Join(b.rootPath, b.Uuid+b.fileExtension)
	} else {
		b.MetaFilepath = path.Join(b.rootPath, b.filenamePrefix+"meta_"+b.Uuid+".json")
		b.DataFilepath = path.Join(b.rootPath, b.filenamePrefix, b.Uuid+b.fileExtension)
	}

	return b
}

func (b *MetadataBuilder[O]) WithMax(m O) *MetadataBuilder[O] {
	if b.Max == nil {
		b.Max = &m
	} else if m > *b.Max {
		*b.Max = m
	}

	return b
}

func (b *MetadataBuilder[O]) WithMin(m O) *MetadataBuilder[O] {
	if b.Min == nil {
		b.Min = &m
	} else if m < *b.Min {
		*b.Min = m
	}

	return b
}

func (b *MetadataBuilder[O]) WithEntry(e Entry[O]) *MetadataBuilder[O] {
	if b.PrimaryIdx == "" {
		b.PrimaryIdx = e.PrimaryIndex()
	}

	if b.Min == nil {
		b.Min = new(O)
		*b.Min = e.Min()
	} else if e.Min() < *b.Min {
		*b.Min = e.Min()
	}

	if b.Max == nil {
		b.Max = new(O)
		*b.Max = e.Max()
	} else if e.Min() < *b.Max {
		*b.Max = e.Max()
	}

	b.ItemCount += e.Len()
	found := false
	for _, row := range b.Rows {
		if row.SecondaryIdx == e.SecondaryIndex() {
			row.Merge(&row)
			return b
		}
	}
	if !found {
		b.Rows = append(b.Rows,
			Row[O]{SecondaryIdx: e.SecondaryIndex(), Min: e.Min(), Max: e.Max(), ItemCount: e.Len()})
	}

	return b
}

// func (b *MetadataBuilder[O]) WithEntries(s string, es Entries[O, Entry[O]]) *MetadataBuilder[O] {
// 	var min, max Entry[O]
//
// 	if es.Len() > 1 {
// 		min = es.Get(0)
// 		max = es.Last()
// 	} else if es.Len() == 1 {
// 		min = es.Get(0)
// 		max = es.Get(0)
// 	}
// 	b.PIdx = es.Get(0).PrimaryIndex()
//
// 	b.ItemCount += es.Len()
// 	b.Rows = append(b.Rows,
// 		Rows[O]{Name: s, Min: min.(T), Max: max.(T), ItemCount: es.Len()})
//
// 	return b
// }

func (b *MetadataBuilder[O]) WithFilename(s string) *MetadataBuilder[O] {
	b.Uuid = s
	return b
}

func (b *MetadataBuilder[O]) WithFilenamePrefix(prefix string) *MetadataBuilder[O] {
	b.filenamePrefix = prefix

	if b.fileExtension == "" {
		b.MetaFilepath = path.Join(b.rootPath, b.filenamePrefix, b.filenamePrefix+"meta_"+b.Uuid+".json")
		b.DataFilepath = path.Join(b.rootPath, b.filenamePrefix, b.Uuid)
	} else {
		b.MetaFilepath = path.Join(b.rootPath, b.filenamePrefix+"meta_"+b.Uuid+".json")
		b.DataFilepath = path.Join(b.rootPath, b.filenamePrefix, b.Uuid+b.fileExtension)
	}

	return b
}

func (b *MetadataBuilder[O]) WithLevel(l int) *MetadataBuilder[O] {
	if l > b.Level {
		b.Level = l
	}

	if b.Level > b.cfg.MaxLevels-1 {
		b.Level = b.cfg.MaxLevels - 1
	}

	return b
}

func (b *MetadataBuilder[O]) Build() (*MetaFile[O], error) {
	if b.MetaFilepath != "" && b.DataFilepath != "" {
		return &b.MetaFile, nil
	}

	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now()
	}

	if b.fileExtension == "" && b.filenamePrefix == "" {
		return nil, errors.New("file extension and / or filename prefix must be set")
	}

	return &b.MetaFile, nil
}
