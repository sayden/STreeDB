package streedb

type Filesystem[T Entry] interface {
	Create(entries Entries[T], level int) (Fileblock[T], error)
	Open(p string) (*MetaFile[T], error)
	Load(*MetaFile[T]) (Entries[T], error)
	Remove(*MetaFile[T]) error
	OpenAllMetaFiles() (Levels[T], error)
}
