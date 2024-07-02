package streedb

type Filesystem[T Entry] interface {
	Create(cfg *Config, entries Entries[T], level int) (Fileblock[T], error)
	Load(Fileblock[T]) (Entries[T], error)
	Merge(a, b Fileblock[T]) (Fileblock[T], error)
	Open(p string) (*MetaFile[T], error)
	OpenAllMetaFiles() (Levels[T], error)
	Remove(Fileblock[T]) error
	UpdateMetadata(Fileblock[T]) error
}
