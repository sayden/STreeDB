package destfs

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sayden/streedb"
)

type s3fs[T streedb.Entry] struct {
	bucket string
	cfg    s3config.Config
	client *s3.Client
}

func InitS3[T streedb.Entry](bucket string) (streedb.DestinationFs[T], streedb.Levels[T], error) {
	streedb.DEFAULT_DB_PATH = bucket

	cfg, err := s3config.LoadDefaultConfig(context.TODO(),
		s3config.WithRegion("us-east-1"),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		return nil, nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8080")
		o.UsePathStyle = true // S3ninja typically requires path-style addressing
	})

	fs := &s3fs[T]{
		bucket: streedb.DEFAULT_DB_PATH,
		cfg:    cfg,
		client: client,
	}

	meta, err := fs.MetaFiles()
	if err != nil {
		return nil, nil, err
	}

	return fs, meta, nil
}

func (s *s3fs[T]) MetaFiles() (streedb.Levels[T], error) {
	levels := streedb.NewLevels[T](streedb.MAX_LEVELS)
	return levels, s.metaFilesInBucket(&levels)
}

func (s *s3fs[T]) metaFilesInBucket(levels *streedb.Levels[T]) error {
	output, err := s.client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String("meta_"),
	})
	if err != nil {
		return err
	}

	for _, object := range output.Contents {
		// log.Infof("key=%s size=%d", *object.Key, *object.Size)
	}

	return nil
}

type s3ObjWrapper struct {
	*s3.GetObjectOutput
}

func (s *s3ObjWrapper) Read(p []byte) (n int, err error) {
	if s.GetObjectOutput != nil {
		return s.Body.Read(p)
	}

	return 0, nil
}

func (s *s3ObjWrapper) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (s *s3ObjWrapper) Close() error {
	if s.GetObjectOutput != nil {
		return s.Body.Close()
	}

	return nil
}

func (s *s3fs[T]) Open(p string) (io.ReadWriteCloser, error) {
	out, err := s.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(p),
	})
	if err != nil {
		return nil, errors.Join(errors.New("error getting obj from S3"), err)
	}

	return &s3ObjWrapper{out}, nil
}

func (s *s3fs[T]) Remove(p string) error {
	return nil
}

func (s *s3fs[T]) Size(a io.ReadWriteCloser) (int64, error) {
	return 0, nil
}
