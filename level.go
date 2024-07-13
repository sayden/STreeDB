package streedb

// type Level[O cmp.Ordered, E Entry[O]] interface {
// 	FileblockListener[O, E]
//
// 	Close() error
// 	Create(es Entries[O, E], builder *MetadataBuilder[O]) (*Fileblock[O, E], error)
// 	Fileblocks() []*Fileblock[O, E]
// 	Find(d E) (Entry[O], bool, error)
// 	FindFileblock(d E) (*Fileblock[O, E], bool, error)
// 	RemoveFile(b *Fileblock[O, E]) error
// }
//
// type Levels[O cmp.Ordered, E Entry[O]] interface {
// 	Level[O, E]
// 	FileblockListener[O, E]
// 	FileblockCreator[O, E]
//
// 	// ForwardIterator(d E) (EntryIterator[E], bool, error)
// 	// RangeIterator(begin, end E) (EntryIterator[E], bool, error)
// 	Level(i int) Level[O, E]
// }
