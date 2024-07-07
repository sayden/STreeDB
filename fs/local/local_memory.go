package fslocal

import db "github.com/sayden/streedb"

func NewMemoryFilesystem[T db.Entry](cfg *db.Config) db.Filesystem[T] {
	return &MemFilesystem[T]{cfg: cfg}
}

type MemFilesystem[T db.Entry] struct {
	cfg *db.Config
}

func (m *MemFilesystem[T]) Create(cfg *db.Config, entries db.Entries[T], meta *db.MetaFile[T], ls []db.FileblockListener[T]) (*db.Fileblock[T], error) {
	block := db.NewFileblock(cfg, meta, m)
	for _, listener := range ls {
		listener.OnNewFileblock(block)
	}
	return block, nil
}
func (m *MemFilesystem[T]) FillMetadataBuilder(meta *db.MetadataBuilder[T]) *db.MetadataBuilder[T] {
	return meta
}

func (m *MemFilesystem[T]) Load(fb *db.Fileblock[T]) (db.Entries[T], error) {
	return nil, nil
}

func (m *MemFilesystem[T]) Remove(block *db.Fileblock[T], ls []db.FileblockListener[T]) error {
	for _, l := range ls {
		l.OnFileblockRemoved(block)
	}
	return nil
}

func (m *MemFilesystem[T]) UpdateMetadata(meta *db.Fileblock[T]) error { return nil }
func (m *MemFilesystem[T]) OpenAllMetaFiles() (db.Levels[T], error)    { return nil, nil }
func (f *MemFilesystem[T]) OpenMetaFilesInLevel(listeners []db.FileblockListener[T]) error {
	return nil
}
