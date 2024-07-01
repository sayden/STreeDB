package fs

import "github.com/sayden/streedb"

func InitMemoryFilesystem[T streedb.Entry](cfg *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	fs := NewMemoryFilesystem[T](cfg)
	return fs, streedb.NewBasicLevels(cfg, fs), nil
}

func NewMemoryFilesystem[T streedb.Entry](cfg *streedb.Config) streedb.Filesystem[T] {
	return &MemFilesystem[T]{cfg: cfg}
}

type MemFilesystem[T streedb.Entry] struct {
	cfg *streedb.Config
}

func (m *MemFilesystem[T]) Create(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	meta, err := streedb.NewMetadataBuilder[T]("").
		WithEntries(entries).
		WithLevel(level).
		Build()
	if err != nil {
		return nil, err
	}
	return NewMemFileblock(entries, level, meta), nil
}

func (m *MemFilesystem[T]) Open(p string) (*streedb.MetaFile[T], error) { return nil, nil }
func (m *MemFilesystem[T]) Load(b streedb.Fileblock[T]) (streedb.Entries[T], error) {
	return nil, nil
}
func (m *MemFilesystem[T]) Remove(meta streedb.Fileblock[T]) error { return nil }
func (m *MemFilesystem[T]) Merge(a, b streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	return nil, nil
}
func (m *MemFilesystem[T]) UpdateMetadata(meta streedb.Fileblock[T]) error { return nil }
func (m *MemFilesystem[T]) OpenAllMetaFiles() (streedb.Levels[T], error)   { return nil, nil }

func NewMemoryFileblockBuilder[T streedb.Entry]() streedb.FileblockBuilder[T] {
	return func(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
		meta, err := streedb.NewMetadataBuilder[T]("").
			WithEntries(entries).
			WithLevel(level).
			Build()
		if err != nil {
			return nil, err
		}
		return NewMemFileblock(entries, level, meta), nil
	}
}

func NewMemFileblock[T streedb.Entry](entries streedb.Entries[T], level int, meta *streedb.MetaFile[T]) *MemFileblock[T] {
	return &MemFileblock[T]{Entries: entries, MetaFile: *meta}
}

type MemFileblock[T streedb.Entry] struct {
	Entries streedb.Entries[T]
	streedb.MetaFile[T]
}

func (m *MemFileblock[T]) Load() (streedb.Entries[T], error)                 { return m.Entries, nil }
func (m *MemFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) { return nil, false, nil }
func (m *MemFileblock[T]) Close() error                                      { return nil }
