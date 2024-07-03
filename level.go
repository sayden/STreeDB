package streedb

type Level[T Entry] interface {
	AppendFileblock(b Fileblock[T]) error
	Close() error
	Create(es Entries[T], meta *MetadataBuilder[T]) error
	Fileblocks() []Fileblock[T]
	Find(d T) (Entry, bool, error)
	Open(p string) (*MetaFile[T], error)
	RemoveFile(b Fileblock[T]) error
}

type Levels[T Entry] interface {
	AppendFileblock(b Fileblock[T]) error
	Close() error
	Create(es Entries[T], initialLevel int) error
	GetLevel(i int) Level[T]
	RemoveFile(b Fileblock[T]) error
}
