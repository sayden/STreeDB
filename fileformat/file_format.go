package fileformat

import "github.com/sayden/streedb"

func NewFileFormat[T streedb.Entry](data streedb.Entries[T], path string, level int) (streedb.Metadata[T], error) {
	return NewParquetBlock(data, path, level)
}
