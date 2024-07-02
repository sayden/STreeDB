package fss3

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

func (f *s3ParquetFs[T]) Load(b streedb.Fileblock[T]) (streedb.Entries[T], error) {
	out, err := f.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(f.cfg.S3Config.Bucket),
		Key:    aws.String(b.Metadata().DataFilepath),
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

func (f *s3ParquetFs[T]) UpdateMetadata(b streedb.Fileblock[T]) error {
	return updateMetadataS3(f.cfg, f.client, f, b.Metadata())
}

func (f *s3ParquetFs[T]) Merge(a, b streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	newEntries, err := streedb.Merge(a, b)
	if err != nil {
		return nil, err
	}
	return f.Create(f.cfg, newEntries, a.Metadata().Level)
}

func (f *s3ParquetFs[T]) Create(cfg *streedb.Config, entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
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

	return NewS3Fileblock(cfg, meta, f), nil
}

func (f *s3ParquetFs[T]) Remove(b streedb.Fileblock[T]) error {
	return removeS3(f.client, f.cfg, b.Metadata())
}

func (f *s3ParquetFs[T]) OpenAllMetaFiles() (streedb.Levels[T], error) {
	return openAllMetadataFilesInS3(f.cfg, f.client, f)
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

	return NewS3Fileblock(cfg, meta, fs), nil
}
