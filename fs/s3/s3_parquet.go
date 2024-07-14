package fss3

import (
	"bufio"
	"bytes"
	"cmp"
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

func InitParquetS3[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, level int) (db.Filesystem[O, E], error) {
	return initS3[O, E](cfg, level)
}

type s3ParquetFs[O cmp.Ordered, E db.Entry[O]] struct {
	cfg      *db.Config
	s3cfg    s3config.Config
	client   *s3.Client
	rootPath string
}

func (f *s3ParquetFs[O, E]) Load(b *db.Fileblock[O, E]) (db.EntriesMap[O, E], error) {
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

	pr, err := reader.NewParquetReader(pf, new(E), db.PARQUET_NUMBER_OF_THREADS)
	if err != nil {
		return nil, err
	}

	numRows := int(pr.GetNumRows())
	entries := make([]E, numRows)
	if err = pr.Read(&entries); err != nil {
		return nil, err
	}

	return db.NewSliceToMapWithMetadata(entries, &b.MetaFile), nil
}

func (f *s3ParquetFs[O, E]) UpdateMetadata(b *db.Fileblock[O, E]) error {
	return updateMetadataS3[O, E](f.cfg, f.client, b.Metadata())
}

func (f *s3ParquetFs[O, E]) Create(cfg *db.Config, es db.EntriesMap[O, E], builder *db.MetadataBuilder[O], ls []db.FileblockListener[O, E]) (*db.Fileblock[O, E], error) {
	if es.SecondaryIndicesLen() == 0 {
		return nil, errors.New("empty data")
	}

	builder = f.FillMetadataBuilder(builder)
	meta, err := builder.Build()
	if err != nil {
		return nil, err
	}

	// data file
	fw, err := s3v2.NewS3FileWriterWithClient(context.TODO(), f.client, f.cfg.S3Config.Bucket, meta.DataFilepath, nil)
	if err != nil {
		return nil, err
	}
	// closed 10 lines below

	parquetWriter, err := writer.NewParquetWriter(fw, new(E), 4)
	if err != nil {
		fw.Close()
		return nil, err
	}

	for _, entry := range es {
		parquetWriter.Write(entry)
	}
	if err = parquetWriter.WriteStop(); err != nil {
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
		l.OnFileblockCreated(block)
	}

	return block, nil
}

func (f *s3ParquetFs[O, E]) Remove(b *db.Fileblock[O, E], listeners []db.FileblockListener[O, E]) error {
	return removeS3(f.client, f.cfg, b, listeners...)
}

func (f *s3ParquetFs[O, E]) OpenMetaFilesInLevel(listeners []db.FileblockListener[O, E]) error {
	return openAllMetadataFilesInS3Folder(f.cfg, f.client, f, f.rootPath, listeners...)
}

func (f *s3ParquetFs[O, E]) FillMetadataBuilder(meta *db.MetadataBuilder[O]) *db.MetadataBuilder[O] {
	return meta.WithRootPath(f.rootPath).WithExtension(".parquet")
}
