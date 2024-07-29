package core

import (
	"cmp"
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thehivecorporation/log"
)

func cleanAll() {
	deleteBuckets()
	os.RemoveAll("/tmp/db")
}

func TestS3(t *testing.T) {
	t.Skip()
	log.SetLevel(log.LevelInfo)
	t.Cleanup(cleanAll)
	defaultCfg := db.NewDefaultConfig()
	defaultCfg.Wal.MaxItems = 10

	cfg := &db.Config{
		Wal:              defaultCfg.Wal,
		Compaction:       defaultCfg.Compaction,
		Filesystem:       db.FilesystemTypeMap[db.FILESYSTEM_TYPE_S3],
		MaxLevels:        5,
		DbPath:           "/tmp/db/s3/parquet",
		LevelFilesystems: []string{"local", "s3", "s3", "s3", "s3"},
		S3Config: db.S3Config{
			Bucket: "parquet",
			Region: "us-east-1",
		},
	}

	createBuckets(t)

	t.Run("Insert", func(t *testing.T) {
		launchTestWithConfig(t, cfg, true)
	})

	t.Run("Compact", func(t *testing.T) {
		launchTestWithConfig(t, cfg, false)
	})
}

func TestDBLocal(t *testing.T) {
	log.SetLevel(log.LevelInfo)
	t.Cleanup(cleanAll)
	defaultCfg := db.NewDefaultConfig()
	defaultCfg.Wal.MaxItems = 5
	defaultCfg.Compaction.Promoters.ItemLimit.MaxItems = 32
	defaultCfg.Compaction.Promoters.ItemLimit.FirstBlockItemCount = 2
	defaultCfg.Compaction.Promoters.ItemLimit.GrowthFactor = 2

	cfg := &db.Config{
		Wal:        defaultCfg.Wal,
		Filesystem: db.FilesystemTypeMap[db.FILESYSTEM_TYPE_LOCAL],
		MaxLevels:  5,
		DbPath:     "/tmp/db/parquet",
		Compaction: defaultCfg.Compaction,
	}

	t.Run("Insert", func(t *testing.T) {
		launchTestWithConfig(t, cfg, true)
	})

	t.Run("Compaction", func(t *testing.T) {
		launchTestWithConfig(t, cfg, false)
	})
}

type mockListener[O cmp.Ordered] struct {
	t *testing.T
}

func (m *mockListener[O]) OnFileblockCreated(fileblock *db.Fileblock[O]) {
	m.t.Logf("Fileblock created with %d on level %v", fileblock.ItemCount, fileblock.Level)
}

func (m *mockListener[O]) OnFileblockRemoved(fileblock *db.Fileblock[O]) {
	m.t.Logf("Fileblock removed with %d on level %v", fileblock.ItemCount, fileblock.Level)
}

func TestDBMemory(t *testing.T) {
	log.SetLevel(log.LevelInfo)
	t.Cleanup(cleanAll)

	cfg := db.NewDefaultConfig()
	cfg.Filesystem = db.FilesystemTypeMap[db.FILESYSTEM_TYPE_MEMORY]
	cfg.LevelFilesystems = nil
	cfg.MaxLevels = 5
	cfg.Wal.MaxItems = 5
	cfg.DbPath = "/tmp/db/parquet"

	cfg.Compaction.Promoters.ItemLimit.MaxItems = 24
	cfg.Compaction.Promoters.ItemLimit.GrowthFactor = 2
	cfg.Compaction.Promoters.ItemLimit.FirstBlockItemCount = 5

	lsmtree, err := NewLsmTree[int64, *db.Kv](cfg, &mockListener[int64]{t})
	require.NoError(t, err)
	defer lsmtree.Close()

	ts := make([]int64, 60)
	vals := make([]int32, 60)
	for i := 0; i < 60; i++ {
		ts[i] = int64(i)
		vals[i] = int32(i)
	}

	for i := 0; i < 60/5; i++ {
		left := i * 5
		right := left + 5

		tss := ts[left:right]
		values := vals[left:right]
		err = lsmtree.Append(db.NewKv("instance1", "cpu", tss, values))
		require.NoError(t, err)
	}

	err = lsmtree.Compact()
	require.NoError(t, err)

	err = lsmtree.Compact()
	require.NoError(t, err)

	err = lsmtree.Compact()
	require.NoError(t, err)

	// with the current config, inserting 60 items and compacting 3 times should result in
	// 1 fileblock at level 4 with 40 items and 1 fileblock at level 3 with 20 items
	lsmtree.levels.Index.Ascend(func(i *db.BtreeItem[int64]) bool {
		i.Val.Each(func(k int, v *db.Fileblock[int64]) bool {
			t.Logf("Fileblock (%d) %s has %d items. Level: %d", k, v.UUID(), v.ItemCount, v.Level)
			switch v.Level {
			case 3:
				assert.Equal(t, 20, v.ItemCount)
			case 4:
				assert.Equal(t, 40, v.ItemCount)
			default:
				t.Fatalf("Unexpected level %d", v.Level)
			}
			return true
		})
		return true
	})
}

func launchTestWithConfig(t *testing.T, cfg *db.Config, insertOrCompact bool) {
	lsmtree, err := NewLsmTree[int64, *db.Kv](cfg)
	require.NoError(t, err)
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

	ts := make([]int64, len(keys))
	for i := 0; i < len(keys); i++ {
		ts[i] = int64(i)
	}

	if insertOrCompact {
		for i := 0; i < len(keys)/5; i++ {
			left := i * 5
			right := left + 5

			values := keys[left:right]
			err = lsmtree.Append(db.NewKv("instance1", "cpu", ts, values))
			require.NoError(t, err)
			err = lsmtree.Append(db.NewKv("instance1", "mem", ts, values))
			require.NoError(t, err)
		}
	}

	err = lsmtree.Close()
	require.NoError(t, err)

	if !insertOrCompact {
		err = lsmtree.Compact()
		require.NoError(t, err)
	}

	val, found, err := lsmtree.Find("instance1", "cpu", 0, 4)
	require.NoError(t, err)
	assert.True(t, found)
	require.NotNil(t, val)
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
