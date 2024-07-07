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
	db "github.com/sayden/streedb"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func InitParquetS3[T db.Entry](cfg *db.Config, level int) (db.Filesystem[T], error) {
	return initS3[T](cfg, level, newS3FilesystemParquet)
}

type s3ParquetFs[T db.Entry] struct {
	cfg      *db.Config
	s3cfg    s3config.Config
	client   *s3.Client
	rootPath string
}

func (f *s3ParquetFs[T]) Load(b *db.Fileblock[T]) (db.Entries[T], error) {
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
	if _, err = file.Seek(0, 0); err != nil {
		return nil, errors.Join(errors.New("error seeking temp file"), err)
	}
	if err = file.Sync(); err != nil {
		return nil, errors.Join(errors.New("error syncing temp file"), err)
	}

	pf, err := local.NewLocalFileReader(file.Name())
	if err != nil {
		return nil, err
	}
	defer pf.Close()

	pr, err := reader.NewParquetReader(pf, new(T), db.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make(db.Entries[T], numRows)
	err = pr.Read(&entries)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func (f *s3ParquetFs[T]) UpdateMetadata(b *db.Fileblock[T]) error {
	return updateMetadataS3(f.cfg, f.client, b.Metadata())
}

func (f *s3ParquetFs[T]) Create(cfg *db.Config, entries db.Entries[T], meta *db.MetaFile[T], ls []db.FileblockListener[T]) (*db.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	// data file
	fw, err := s3v2.NewS3FileWriterWithClient(context.TODO(), f.client, f.cfg.S3Config.Bucket, meta.DataFilepath, nil)
	if err != nil {
		return nil, err
	}
	// closed 10 lines below

	pw, err := writer.NewParquetWriter(fw, new(T), 4)
	if err != nil {
		fw.Close()
		return nil, err
	}
	for _, entry := range entries {
		pw.Write(entry)
	}

	if err = pw.WriteStop(); err != nil {
		fw.Close()
		return nil, err
	}
	if err = fw.Close(); err != nil {
		return nil, err
	}

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
	block := db.NewFileblock(cfg, meta, f)
	for _, l := range ls {
		l.OnNewFileblock(block)
	}

	return block, nil
}

func (f *s3ParquetFs[T]) Remove(b *db.Fileblock[T], listeners []db.FileblockListener[T]) error {
	return removeS3(f.client, f.cfg, b, listeners...)
}

func (f *s3ParquetFs[T]) OpenMetaFilesInLevel(listeners []db.FileblockListener[T]) error {
	return openAllMetadataFilesInS3Folder(f.cfg, f.client, f, f.rootPath, listeners...)
}

func (f *s3ParquetFs[T]) FillMetadataBuilder(meta *db.MetadataBuilder[T]) *db.MetadataBuilder[T] {
	return meta.WithRootPath(f.rootPath).WithExtension(".parquet")
}
