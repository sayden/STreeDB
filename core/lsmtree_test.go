package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/thehivecorporation/log"
)

func TestLocalJSON(t *testing.T) {
	log.SetLevel(log.LevelInfo)

	cfg := &streedb.Config{
		WalMaxItems: 5,
		DbPath:      "/tmp/kv",
		Filesystem:  streedb.FILESYSTEM_LOCAL,
		Format:      streedb.FILE_FORMAT_JSON,
		MaxLevels:   5,
	}

	// lsmtree, err := NewLsmTree[streedb.Kv]("/tmp/kv", destfs.DEST_FS_LOCAL, walSize)
	lsmtree, err := NewLsmTree[streedb.Kv](cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree.Close()
	compact := false

	// compact = true
	// r := rand.New(rand.NewSource(42))
	// n := r.Int31()
	// 	// lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", n), Val: n})

	var i int32
	for i < 25 {
		lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", i), Val: i})
		i++
	}

	// tree := btree.NewG(2,
	// 	func(a, b streedb.MetaFile[streedb.Kv]) bool {
	// 		return a.MinVal.LessThan(b.MinVal)
	// 	})
	//
	// for _, block := range lsmtree.levels.GetLevel(0) {
	// 	b := block.(*fileformat.LocalBlockJSON[streedb.Kv])
	// 	tree.ReplaceOrInsert(b.MetaFile)
	// }
	//
	// min := streedb.MetaFile[streedb.Kv]{
	// 	MinVal: streedb.Kv{Key: "hello 06", Val: 0},
	// }
	// max := streedb.MetaFile[streedb.Kv]{
	// 	MinVal: streedb.Kv{Key: "hello 19", Val: 0},
	// }
	//
	// tree.DescendLessOrEqual(min, func(item streedb.MetaFile[streedb.Kv]) bool {
	// 	fmt.Printf("item: %#v, %#v\n", item.MinVal, item.MaxVal)
	// 	return false
	// })
	//
	// tree.AscendRange(min, max, func(item streedb.MetaFile[streedb.Kv]) bool {
	// 	fmt.Printf("item: %#v, %#v\n", item.MinVal, item.MaxVal)
	// 	return true
	// })

	if compact {
		err = lsmtree.Compact()
		assert.NoError(t, err)
	}

	entry := streedb.NewLexicographicKv("hello 06", 0)
	val, found, err := lsmtree.Find(*entry)
	assert.NoError(t, err)
	if !found {
		t.Fatal("value not found")
	}
	assert.Equal(t, int32(6), val.(streedb.Kv).Val)
}

func TestS3(t *testing.T) {
	createBuckets()

	log.SetLevel(log.LevelInfo)

	cfgs := []*streedb.Config{
		{
			WalMaxItems: 5,
			Filesystem:  streedb.FILESYSTEM_S3,
			Format:      streedb.FILE_FORMAT_PARQUET,
			MaxLevels:   5,
			S3Config: streedb.S3Config{
				Bucket: "parquet",
				Region: "us-east-1",
			},
		},
		{
			WalMaxItems: 5,
			Filesystem:  streedb.FILESYSTEM_S3,
			Format:      streedb.FILE_FORMAT_JSON,
			MaxLevels:   5,
			S3Config: streedb.S3Config{
				Bucket: "json",
				Region: "us-east-1",
			},
		},
	}

	testF := func(cfg *streedb.Config, insert bool) {
		lsmtree, err := NewLsmTree[streedb.Kv](cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer lsmtree.Close()

		compact := false

		// compact = true
		if insert {
			var i int32
			for i < 25 {
				lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", i), Val: i})
				i++
			}
		}

		if compact {
			err = lsmtree.Compact()
			assert.NoError(t, err)
		}

		entry := streedb.NewLexicographicKv("hello 06", 0)
		val, found, err := lsmtree.Find(*entry)
		assert.NoError(t, err)
		if !found {
			t.Errorf("value not found in '%s' using '%s'", streedb.FilesystemMap[cfg.Filesystem], streedb.FormatMap[cfg.Format])
		} else {
			assert.Equal(t, int32(6), val.(streedb.Kv).Val)
		}
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
