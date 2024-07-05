package fslocal

import db "github.com/sayden/streedb"

func NewMemoryFilesystem[T db.Entry](cfg *db.Config) db.Filesystem[T] {
	return &MemFilesystem[T]{cfg: cfg}
}

type MemFilesystem[T db.Entry] struct {
	cfg *db.Config
}

func (m *MemFilesystem[T]) Create(cfg *db.Config, entries db.Entries[T], meta *db.MetaFile[T]) (db.Fileblock[T], error) {
	return NewMemFileblock(entries, meta, m), nil
}
func (m *MemFilesystem[T]) FillMetadataBuilder(meta *db.MetadataBuilder[T]) *db.MetadataBuilder[T] {
	return meta
}

func (m *MemFilesystem[T]) Open(p string) (*db.MetaFile[T], error) { return nil, nil }
func (m *MemFilesystem[T]) Load(fb db.Fileblock[T]) (db.Entries[T], error) {
	return fb.(*MemFileblock[T]).Entries, nil
}
func (m *MemFilesystem[T]) Remove(meta db.Fileblock[T]) error         { return nil }
func (m *MemFilesystem[T]) UpdateMetadata(meta db.Fileblock[T]) error { return nil }
func (m *MemFilesystem[T]) OpenAllMetaFiles() (db.Levels[T], error)   { return nil, nil }
func (f *MemFilesystem[T]) OpenMetaFilesInLevel(level db.Level[T]) error {
	return nil
}

func NewMemFileblock[T db.Entry](entries db.Entries[T], meta *db.MetaFile[T], f db.Filesystem[T]) *MemFileblock[T] {
	return &MemFileblock[T]{Entries: entries, MetaFile: *meta, Filesystem: f}
}

type MemFileblock[T db.Entry] struct {
	Entries db.Entries[T]
	db.MetaFile[T]
	db.Filesystem[T]
}

func (m *MemFileblock[T]) Load() (db.Entries[T], error) { return m.Entries, nil }
func (m *MemFileblock[T]) Find(v db.Entry) bool         { return false }
func (m *MemFileblock[T]) Close() error                 { return nil }
func (m *MemFileblock[T]) RootPath() string             { return "" }
