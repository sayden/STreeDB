package streedb

import "io"

var (
	DEFAULT_DB_PATH = "/tmp/test"
)

type DestinationFs[T Entry] interface {
	MetaFiles() (Levels[T], error)
	Open(p string) (io.ReadWriteCloser, error)
	Remove(p string) error
	Size(io.ReadWriteCloser) (int64, error)
	Create(p string) (io.ReadWriteCloser, error)
}
