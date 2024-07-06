package streedb

type Level[T Entry] interface {
	FileblockListener[T]

	Close() error
	Create(es Entries[T], meta *MetadataBuilder[T]) (Fileblock[T], error)
	Fileblocks() []Fileblock[T]
	Find(d T) (Entry, bool, error)
	FindFileblock(d T) (Fileblock[T], bool, error)
	RemoveFile(b Fileblock[T]) error
}

type Levels[T Entry] interface {
	Level[T]
	FileblockListener[T]

	NewFileblock(es Entries[T], initialLevel int) error
	ForwardIterator(d T) (EntryIterator[T], bool, error)
	RangeIterator(begin, end T) (EntryIterator[T], bool, error)
	Level(i int) Level[T]
}
