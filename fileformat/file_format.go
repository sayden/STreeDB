package fileformat

import "github.com/sayden/streedb"

const (
	FILE_FORMAT_PARQUET = "parquet"
	FILE_FORMAT_JSON    = "json"
)

const FILE_FORMAT = FILE_FORMAT_JSON

func NewFileFormat[T streedb.Entry](data streedb.Entries[T], level int) (streedb.Metadata[T], error) {
	switch FILE_FORMAT {
	case FILE_FORMAT_JSON:
		return NewJSONBlock(data, level)
	case FILE_FORMAT_PARQUET:
		return NewParquetBlock(data, level)
	}

	return NewParquetBlock(data, level)
}

func NewEmptyFormat[T streedb.Entry](min, max *T, filepath string) (streedb.Metadata[T], error) {
	switch FILE_FORMAT {
	case FILE_FORMAT_JSON:
		return NewEmptyJSONBlock(min, max, filepath)
	case FILE_FORMAT_PARQUET:
		return NewEmptyParquetBlock(min, max, filepath)
	}

	return NewEmptyParquetBlock(min, max, filepath)
}
