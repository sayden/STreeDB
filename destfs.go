package streedb

var (
	DEFAULT_DB_PATH = "/tmp/test"
)

type DestinationFs[T Entry] interface {
	MetaFiles() (Levels[T], error)
}
