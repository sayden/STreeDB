package core

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/thehivecorporation/log"
)

func TestDev(t *testing.T) {
	log.SetLevel(log.LevelInfo)

	cfgs := []*streedb.Config{
		{
			WalMaxItems:           5,
			Filesystem:            streedb.FilesystemTypeMap[streedb.FILESYSTEM_TYPE_LOCAL],
			Format:                streedb.FormatMap[streedb.FILE_FORMAT_JSON],
			MaxLevels:             5,
			DbPath:                "/tmp/kv/json",
			CompactionExtraPasses: 1,
			LevelFilesystems:      []string{"local", "local", "local", "local", "local"},
		},
	}

	testF := func(cfg *streedb.Config, insert bool) {
		lsmtree, err := NewLsmTree[streedb.Kv](cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer lsmtree.Close()

		compact := false

		compact = true
		keys := []int{
			1, 2, 4, 5, 6,
			3, 7, 7, 8, 8,
			10, 11, 12, 13, 14,
			15, 11, 17, 18, 19,
			20, 21, 22, 23, 24,
			25, 26, 16, 27, 28,
			29, 44, 45, 36, 59,
			60, 61, 62,
		}
		totalKeys := int32(len(keys))

		if insert {
			var i int32
			for _, k := range keys {
				lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", k), Val: i})
				i++
			}
		}

		if compact {
			err = lsmtree.Compact()
			assert.NoError(t, err)
		}

		entry := streedb.NewKv("hello 07", 0)
		val, found, err := lsmtree.Find(entry)
		assert.NoError(t, err)
		assert.True(t, found, "value not found in '%s' using '%s'",
			streedb.FilesystemTypeReverseMap[cfg.Filesystem], streedb.ReverseFormatMap[cfg.Format])
		assert.True(t, val.(streedb.Kv).Val >= int32(0) && val.(streedb.Kv).Val <= totalKeys)
	}

	for _, cfg := range cfgs {
		testF(cfg, true)
		testF(cfg, false)
	}
}

func TestDBs(t *testing.T) {
	createBuckets()

	log.SetLevel(log.LevelInfo)

	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	cfgs := []*streedb.Config{
		// {
		// 	WalMaxItems:      5,
		// 	Filesystem:       streedb.FilesystemTypeMap[streedb.FILESYSTEM_TYPE_S3],
		// 	Format:           streedb.FormatMap[streedb.FILE_FORMAT_PARQUET],
		// 	MaxLevels:        5,
		// 	LevelFilesystems: []string{"local", "s3", "s3", "s3", "s3"},
		// 	S3Config: streedb.S3Config{
		// 		Bucket: "parquet",
		// 		Region: "us-east-1",
		// 	},
		// 	DbPath: "/tmp/kv/parquet",
		// },
		{
			WalMaxItems:      5,
			LevelFilesystems: []string{"local", "s3", "s3", "s3", "s3"},
			Filesystem:       streedb.FilesystemTypeMap[streedb.FILESYSTEM_TYPE_S3],
			Format:           streedb.FormatMap[streedb.FILE_FORMAT_JSON],
			MaxLevels:        5,
			DbPath:           "/tmp/kv/json",
			S3Config: streedb.S3Config{
				Bucket: "json",
				Region: "us-east-1",
			},
		},
		// {
		// 	WalMaxItems: 5,
		// 	Filesystem:  streedb.FilesystemTypeMap[streedb.FILESYSTEM_TYPE_LOCAL],
		// 	Format:      streedb.FormatMap[streedb.FILE_FORMAT_JSON],
		// 	MaxLevels:   5,
		// 	DbPath:      tmpDir + "/json",
		// },
		// {
		// 	WalMaxItems: 5,
		// 	Filesystem:  streedb.FilesystemTypeMap[streedb.FILESYSTEM_TYPE_LOCAL],
		// 	Format:      streedb.FormatMap[streedb.FILE_FORMAT_PARQUET],
		// 	MaxLevels:   5,
		// 	DbPath:      tmpDir + "/parquet",
		// },
	}

	testF := func(cfg *streedb.Config, insert bool) {
		lsmtree, err := NewLsmTree[streedb.Kv](cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer lsmtree.Close()

		compact := false

		compact = true
		keys := []int{
			1, 2, 4, 5, 6,
			3, 7, 7, 8, 8,
			10, 11, 12, 13, 14,
			15, 11, 17, 18, 19,
			20, 21, 22, 23, 24,
			25, 26, 16, 27, 28,
			29, 44, 45, 36, 59,
			60, 61, 62,
		}

		total := int32(len(keys))
		if insert {
			var i int32
			for _, k := range keys {
				lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", k), Val: i})
				i++
			}
		}

		if compact {
			err = lsmtree.Compact()
			assert.NoError(t, err)
		}

		entry := streedb.NewKv("hello 15", 0)
		val, found, err := lsmtree.Find(entry)
		assert.NoError(t, err)
		assert.True(t, found, "value not found in '%s' using '%s'",
			streedb.FilesystemTypeReverseMap[cfg.Filesystem], streedb.ReverseFormatMap[cfg.Format])
		assert.True(t, val.(streedb.Kv).Val >= int32(0) && val.(streedb.Kv).Val <= total)
	}

	for _, cfg := range cfgs {
		testF(cfg, true)
		testF(cfg, false)
	}
}

func createBuckets() {
	// Load the default AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		log.Fatalf("Unable to load SDK config, %v", err)
	}

	// Create an S3 client with custom endpoint
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8080")
		o.UsePathStyle = true // S3ninja typically requires path-style addressing
	})
	client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String("json"),
	})
	client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String("parquet"),
	})

	// Use the client to interact with S3ninja
	listResult, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		log.Fatalf("Unable to list buckets, %v", err)
	}

	log.Debug("Buckets:")
	for _, bucket := range listResult.Buckets {
		log.Debugf("* %s found on %s\n", aws.ToString(bucket.Name), bucket.CreationDate)
	}
}
