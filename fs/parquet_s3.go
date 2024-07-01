package fs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"

	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/sayden/streedb"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func InitParquetS3[T streedb.Entry](cfg *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	return initS3[T](cfg, newS3FilesystemParquet)
}

type s3ParquetFs[T streedb.Entry] struct {
	cfg    *streedb.Config
	s3cfg  s3config.Config
	client *s3.Client
}

func (s *s3ParquetFs[T]) Open(p string) (*streedb.MetaFile[T], error) {
	return openS3[T](s.client, s.cfg, p)
}

func (f *s3ParquetFs[T]) Load(m *streedb.MetaFile[T]) (streedb.Entries[T], error) {
	out, err := f.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(f.cfg.S3Config.Bucket),
		Key:    aws.String(m.DataFilepath),
	})
	if err != nil {
		return nil, errors.Join(errors.New("error getting obj from S3"), err)
	}
	defer out.Body.Close()

	// FIXME: HACK! Write to a temp file to be able to read it with parquet-go
	// Probably there is a better way to do this but I couldn't find it
	// Everything from here til the end of the function must be refactored
	// Parquet library provides a way to read from a S3 object, but it's returning
	// a seek error everytime I try to read from it and I coudn't find a way to fix it
	file, err := os.CreateTemp("/tmp", "parquet")
	if err != nil {
		return nil, errors.Join(errors.New("error creating temp file"), err)
	}
	defer file.Close()
	defer os.Remove(file.Name())

	if _, err = bufio.NewReader(out.Body).WriteTo(file); err != nil {
		return nil, errors.Join(errors.New("error writing to temp file"), err)
	}
	_ = file.Sync()

	pf, err := local.NewLocalFileReader(file.Name())
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	pr, err := reader.NewParquetReader(pf, new(T), streedb.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make(streedb.Entries[T], numRows)
	err = pr.Read(&entries)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func (f *s3ParquetFs[T]) Merge(a, b streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	newEntries, err := merge(a, b)
	if err != nil {
		return nil, err
	}
	return f.Create(newEntries, a.Metadata().Level)
}

func (f *s3ParquetFs[T]) Create(entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T]("").
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".parquet").
		Build()
	if err != nil {
		return nil, errors.Join(errors.New("error creating metadata"), err)
	}

	// data file
	fw, err := s3v2.NewS3FileWriterWithClient(context.TODO(), f.client, f.cfg.S3Config.Bucket, meta.DataFilepath, nil)
	if err != nil {
		return nil, err
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(T), 4)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		pw.Write(entry)
	}

	if err = pw.WriteStop(); err != nil {
		return nil, err
	}
	fw.Close()

	// get size
	stat, err := f.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(f.cfg.S3Config.Bucket),
		Key:    aws.String(meta.DataFilepath),
	})
	if err != nil {
		return nil, errors.Join(errors.New("error getting obj size from S3"), err)
	}
	meta.Size = *stat.ContentLength

	byt, err := json.Marshal(meta)
	if err != nil {
		return nil, errors.Join(errors.New("error encoding entries"), err)
	}

	// meta file
	if _, err = f.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(f.cfg.S3Config.Bucket),
		Key:    aws.String(meta.MetaFilepath),
		Body:   bytes.NewReader(byt),
	}); err != nil {
		f.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(f.cfg.S3Config.Bucket),
			Key:    aws.String(meta.DataFilepath),
		})
		return nil, errors.Join(errors.New("error putting obj to S3"), err)
	}

	return &s3ParquetFileblock[T]{MetaFile: *meta, fs: f}, nil
}

func (f *s3ParquetFs[T]) Remove(m *streedb.MetaFile[T]) error {
	return removeS3[T](f.client, f.cfg, m)
}

func (f *s3ParquetFs[T]) OpenAllMetaFiles() (streedb.Levels[T], error) {
	return openAllMetadataFilesInS3[T](f.cfg, f.client, f, newS3FileblockParquet)
}

// s3ParquetFileblock works using plain JSON files to store data (and metadata).
type s3ParquetFileblock[T streedb.Entry] struct {
	streedb.MetaFile[T]

	cfg *streedb.Config
	fs  streedb.Filesystem[T]
}

func (l *s3ParquetFileblock[T]) Load() (streedb.Entries[T], error) {
	return l.fs.Load(&l.MetaFile)
}

func (l *s3ParquetFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInsideMinMax(l.Min, l.Max, v) {
		return nil, false, nil
	}

	entries, err := l.Load()
	if err != nil {
		return nil, false, errors.Join(errors.New("error loading block"), err)
	}

	entry, found := entries.Find(v)

	return entry, found, nil
}

func (l *s3ParquetFileblock[T]) Metadata() *streedb.MetaFile[T] {
	return &l.MetaFile
}

func (l *s3ParquetFileblock[T]) Close() error {
	//noop
	return nil
}

func newParquetS3Fileblock[T streedb.Entry](entries streedb.Entries[T], cfg *streedb.Config, level int, fs streedb.Filesystem[T]) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T]("").
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".parquet").
		Build()
	if err != nil {
		return nil, err
	}

	return &s3ParquetFileblock[T]{
		cfg:      cfg,
		MetaFile: *meta,
		fs:       fs,
	}, nil
}
