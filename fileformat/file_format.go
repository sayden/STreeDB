package fileformat

import "github.com/sayden/streedb"

const (
	FILE_FORMAT_PARQUET = "parquet"
	FILE_FORMAT_JSON    = "json"
)

const FILE_FORMAT = FILE_FORMAT_JSON

func NewFile[T streedb.Entry](data streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	switch FILE_FORMAT {
	case FILE_FORMAT_JSON:
		return NewJSONFileblock(data, level)
	case FILE_FORMAT_PARQUET:
		return NewParquetBlock(data, level)
	}

	return NewParquetBlock(data, level)
}

func NewReadOnlyFile[T streedb.Entry](filepath string) (streedb.Fileblock[T], error) {
	switch FILE_FORMAT {
	case FILE_FORMAT_JSON:
		return NewReadOnlyJSONFileblock[T](filepath)
	case FILE_FORMAT_PARQUET:
		return NewReadOnlyParquetFile[T](filepath)
	}

	return NewReadOnlyParquetFile[T](filepath)
}
