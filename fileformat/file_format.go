package fileformat

import "github.com/sayden/streedb"

const (
	FILE_FORMAT_PARQUET = "parquet"
	FILE_FORMAT_JSON    = "json"
)

const FILE_FORMAT = FILE_FORMAT_JSON

func NewFile[T streedb.Entry](data streedb.Entries[T], level int, fs streedb.DestinationFs[T]) (streedb.Fileblock[T], error) {
	switch FILE_FORMAT {
	case FILE_FORMAT_JSON:
		return NewJSONFileblock(data, level, fs)
	case FILE_FORMAT_PARQUET:
		return NewParquetBlock(data, level, fs)
	}

	return NewParquetBlock(data, level, fs)
}

func NewReadOnlyFile[T streedb.Entry](filepath string, fs streedb.DestinationFs[T]) (streedb.Fileblock[T], error) {
	switch FILE_FORMAT {
	case FILE_FORMAT_JSON:
		return NewReadOnlyJSONFileblock[T](filepath, fs)
	case FILE_FORMAT_PARQUET:
		return NewReadOnlyParquetFile[T](filepath, fs)
	}

	return NewReadOnlyParquetFile[T](filepath, fs)
}
