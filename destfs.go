package streedb

type DestinationFs[T Entry] interface {
	MetaFiles() (Levels[T], error)
}
