package core

import (
	"context"
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

func cleanAll() {
	// deleteBuckets()
	os.RemoveAll("/tmp/db")
}

func TestS3(t *testing.T) {
	log.SetLevel(log.LevelInfo)
	// t.Cleanup(cleanAll)
	defaultCfg := streedb.NewDefaultConfig()
	defaultCfg.Wal.MaxItems = 10

	testCfgs := []*streedb.Config{
		{
			Wal:              defaultCfg.Wal,
			Compaction:       defaultCfg.Compaction,
			Filesystem:       streedb.FilesystemTypeMap[streedb.FILESYSTEM_TYPE_S3],
			MaxLevels:        5,
			DbPath:           "/tmp/db/s3/parquet",
			LevelFilesystems: []string{"local", "s3", "s3", "s3", "s3"},
			S3Config: streedb.S3Config{
				Bucket: "parquet",
				Region: "us-east-1",
			},
		},
	}

	for _, cfg := range testCfgs {
		createBuckets(t)

		t.Run("Insert", func(t *testing.T) {
			launchTestWithConfig(t, cfg, true)
		})

		t.Run("Compact", func(t *testing.T) {
			t.Skip()
			launchTestWithConfig(t, cfg, false)
		})
	}
}

func TestDBLocal(t *testing.T) {
	log.SetLevel(log.LevelInfo)
	t.Cleanup(cleanAll)
	defaultCfg := streedb.NewDefaultConfig()
	defaultCfg.Wal.MaxItems = 5

	testCfgs := []*streedb.Config{
		{
			Wal:        defaultCfg.Wal,
			Filesystem: streedb.FilesystemTypeMap[streedb.FILESYSTEM_TYPE_LOCAL],
			MaxLevels:  5,
			DbPath:     "/tmp/db/parquet",
			Compaction: defaultCfg.Compaction,
		},
	}

	for _, cfg := range testCfgs {
		t.Run("Insert", func(t *testing.T) {
			launchTestWithConfig(t, cfg, true)
		})

		t.Run("Compaction", func(t *testing.T) {
			launchTestWithConfig(t, cfg, false)
		})
	}
}

func launchTestWithConfig(t *testing.T, cfg *streedb.Config, insertOrCompact bool) {
	lsmtree, err := NewLsmTree[int32, *streedb.Kv](cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree.Close()

	keys := []int32{
		1, 2, 4, 5, 6,
		3, 7, 7, 8, 8,
		10, 11, 12, 13, 14,
		10, 11, 12, 13, 14,
		10, 11, 12, 13, 14,
		10, 11, 12, 13, 14,
		10, 11, 12, 13, 14,
		15, 11, 17, 18, 19,
		20, 21, 22, 23, 24,
		25, 26, 16, 27, 28,
		25, 26, 16, 27, 28,
		29, 44, 45, 36, 59,
		60, 61, 62, 63,
	}

	if insertOrCompact {
		lsmtree.Append(streedb.NewKv("hello", keys, "a"))
		lsmtree.Append(streedb.NewKv("hello", keys, "world"))
	}

	if insertOrCompact {
		lsmtree.Append(streedb.NewKv("hello", keys, "a"))
		lsmtree.Append(streedb.NewKv("hello", keys, "world"))
	}

	lsmtree.Close()

	if !insertOrCompact {
		err = lsmtree.Compact()
		assert.NoError(t, err)
	}

	// entry := streedb.NewKv("hello", nil, "a")
	// val, found, err := lsmtree.Find(entry)
	// assert.NoError(t, err)
	// assert.True(t, found)
	// if val == nil {
	// 	t.Fatalf("value not found in '%s'", cfg.Filesystem)
	// }

	// t.Run("Iterators", func(t *testing.T) {
	// 	t.Skip("TODO")
	//
	// 	begin := streedb.NewKv("hello 27", 0, "a")
	//
	// 	t.Run("ForwardIterator", func(t *testing.T) {
	// 		iter, found, err := lsmtree.ForwardIterator(begin)
	// 		assert.NoError(t, err)
	// 		if !found {
	// 			t.Fatalf("(ForwardIterator) value '%s' not found in '%s' using '%s'", begin.Key, cfg.Filesystem, cfg.Format)
	// 		}
	//
	// 		for val, found, err = iter.Next(); err == nil && found; val, found, err = iter.Next() {
	// 			t.Logf("val: %v", val)
	// 		}
	// 		if err != nil {
	// 			if err != io.EOF {
	// 				t.Fatalf("error iterating over values: %v", err)
	// 			}
	// 		}
	// 	})
	//
	// 	t.Run("RangeIterators", func(t *testing.T) {
	// 		end := streedb.NewKv("hello 39", 0, "a")
	// 		iter, found, err := lsmtree.RangeIterator(begin, end)
	// 		assert.NoError(t, err)
	// 		if !found {
	// 			t.Fatalf("(RangeIterator) value '%s' not found in '%s' using '%s'", begin.Key, cfg.Filesystem, cfg.Format)
	// 		}
	//
	// 		for val, found, err = iter.Next(); err == nil && found; val, found, err = iter.Next() {
	// 			t.Logf("val: %v", val)
	// 		}
	// 		if err != nil {
	// 			if err != io.EOF {
	// 				t.Fatalf("error iterating over values: %v", err)
	// 			}
	// 		}
	// 	})
	// })
}

func deleteBuckets() {
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

	DeleteBucketAndContents(client, "json")
	DeleteBucketAndContents(client, "parquet")

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

func createBuckets(t *testing.T) {
	// Load the default AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		t.Fatalf("Unable to load SDK config, %v", err)
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
		t.Fatalf("Unable to list buckets, %v", err)
	}

	log.Debug("Buckets:")
	for _, bucket := range listResult.Buckets {
		t.Logf("* %s found on %s\n", aws.ToString(bucket.Name), bucket.CreationDate)
	}
}

func DeleteBucketAndContents(client *s3.Client, bucketName string) {
	// First, delete all objects in the bucket
	err := DeleteObjects(client, bucketName)
	if err != nil {
		panic(err)
	}

	// Then, delete the empty bucket
	_, _ = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
}

func DeleteObjects(client *s3.Client, bucketName string) error {
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	paginator := s3.NewListObjectsV2Paginator(client, listInput)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			// log.Errorf("error getting paginator to list objects in S3: %v\n", err)
			break
		}

		for _, object := range page.Contents {
			_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    object.Key,
			})
			if err != nil {
				// log.Errorf("Couldn't delete object %v. Here's why: %v\n", *object.Key, err)
				return err
			}
		}
	}

	return nil
}
