package streedb

import "time"

func NewDefaultConfig() *Config {
	return &Config{
		MaxLevels:        5,
		DbPath:           "/tmp/db",
		Filesystem:       FilesystemTypeMap[FILESYSTEM_TYPE_LOCAL],
		LevelFilesystems: []string{"local", "local", "local", "local", "local"},
		Wal: WalCfg{
			MaxItems:         100,
			MaxElapsedTimeMs: time.Hour.Milliseconds() * 1000,
			MaxSizeBytes:     32 * 1024,
		},
		Compaction: CompactionCfg{
			Promoters: PromotersCfg{
				TimeLimit: TimeLimitPromoterCfg{
					GrowthFactor: 8,
					MaxTimeMs:    7 * 24 * 3600 * 1000,
					MinTimeMs:    1000 * 3600,
				},
				SizeLimit: SizeLimitPromoterCfg{
					GrowthFactor:        16,
					FirstBlockSizeBytes: 1024 * 1024,
				},
				ItemLimit: ItemLimitPromoterCfg{
					GrowthFactor: 8,
					MaxItems:     1000,
				},
			},
		},
	}

}

type Config struct {
	MaxLevels        int
	DbPath           string
	Filesystem       string
	S3Config         S3Config
	LevelFilesystems []string
	Compaction       CompactionCfg
	Wal              WalCfg
}

type WalCfg struct {
	MaxItems         int
	MaxElapsedTimeMs int64
	MaxSizeBytes     int
}

type CompactionCfg struct {
	Promoters PromotersCfg
}

type PromotersCfg struct {
	TimeLimit TimeLimitPromoterCfg
	SizeLimit SizeLimitPromoterCfg
	ItemLimit ItemLimitPromoterCfg
}

type SizeLimitPromoterCfg struct {
	GrowthFactor        int
	FirstBlockSizeBytes int
	MaxBlockSizeBytes   int
}

type ItemLimitPromoterCfg struct {
	GrowthFactor int
	MaxItems     int
}

type TimeLimitPromoterCfg struct {
	GrowthFactor int
	MaxTimeMs    int64
	MinTimeMs    int64
}

type S3Config struct {
	Region string
	Bucket string
}
