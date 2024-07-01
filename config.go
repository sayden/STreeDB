package streedb

type FILE_FORMAT int
type FILESYSTEM int

const (
	FILE_FORMAT_JSON = iota
	FILE_FORMAT_PARQUET

	FILESYSTEM_LOCAL = iota
	FILESYSTEM_S3
)

const (
	CURRENT_FILE_FORMAT = FILE_FORMAT_JSON
	CURRENT_FILESYSTEM  = FILESYSTEM_LOCAL
)

const (
	PARQUET_NUMBER_OF_THREADS = 8
)

type Config struct {
	CompactionPasses int
	MaxLevels        int
	DbPath           string
	Filesystem       FILESYSTEM
	Format           FILE_FORMAT
	WalMaxItems      int
	S3Config         S3Config
}

type S3Config struct {
	Region string
	Bucket string
}

var FormatMap = map[FILE_FORMAT]string{
	FILE_FORMAT_JSON:    "JSON",
	FILE_FORMAT_PARQUET: "PARQUET",
}

var FilesystemMap = map[FILESYSTEM]string{
	FILESYSTEM_LOCAL: "LOCAL",
	FILESYSTEM_S3:    "S3",
}
