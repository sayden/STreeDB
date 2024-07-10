package streedb

type FILE_FORMAT int

const (
	FILE_FORMAT_JSON FILE_FORMAT = iota
	FILE_FORMAT_PARQUET
)

var FormatMap = map[FILE_FORMAT]string{
	FILE_FORMAT_JSON:    "json",
	FILE_FORMAT_PARQUET: "parquet",
}

var ReverseFormatMap = map[string]FILE_FORMAT{
	"json":    FILE_FORMAT_JSON,
	"parquet": FILE_FORMAT_PARQUET,
}

const (
	PARQUET_NUMBER_OF_THREADS = 8
)
