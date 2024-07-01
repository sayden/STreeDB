package streedb

// Fileblock represents a block of data that is written to disk.
// A block of data is a list of Entries, defined by the entries.go file.
// Implementations of Fileblock should be able to read and write data into their respective
// storage formats and retrieve the metadata associated with their contents like Min or Max. They
// must not know about the destination of the data files and work mostly with io.Readers and
// io.Writers
type Fileblock[T Entry] interface {
	DataOps[T]
	Metadata() *MetaFile[T]
}

type DataOps[T Entry] interface {
	Load() (Entries[T], error)
	Find(v Entry) (Entry, bool, error)
	Close() error
}

type FileblockBuilder[T Entry] func(cfg *Config, entries Entries[T], level int) (Fileblock[T], error)
