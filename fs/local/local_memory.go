package fslocal

import db "github.com/sayden/streedb"

func NewMemoryFilesystem[T db.Entry](cfg *db.Config) db.Filesystem[T] {
	return &MemFilesystem[T]{cfg: cfg}
}

type MemFilesystem[T db.Entry] struct {
	cfg *db.Config
}

func (m *MemFilesystem[T]) Create(cfg *db.Config, entries db.Entries[T], meta *db.MetaFile[T], ls []db.FileblockListener[T]) (db.Fileblock[T], error) {
	block := NewMemFileblock(entries, meta, m)
	for _, l := range ls {
		l.OnNewFileblock(block)
	}
	return block, nil
}
func (m *MemFilesystem[T]) FillMetadataBuilder(meta *db.MetadataBuilder[T]) *db.MetadataBuilder[T] {
	return meta
}

func (m *MemFilesystem[T]) Load(fb db.Fileblock[T]) (db.Entries[T], error) {
	return fb.(*MemFileblock[T]).Entries, nil
}

func (m *MemFilesystem[T]) Remove(block db.Fileblock[T], ls []db.FileblockListener[T]) error {
	for _, l := range ls {
		l.OnFileblockRemoved(block)
	}
	return nil
}

func (m *MemFilesystem[T]) UpdateMetadata(meta db.Fileblock[T]) error { return nil }
func (m *MemFilesystem[T]) OpenAllMetaFiles() (db.Levels[T], error)   { return nil, nil }
func (f *MemFilesystem[T]) OpenMetaFilesInLevel(listeners []db.FileblockListener[T]) error {
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
