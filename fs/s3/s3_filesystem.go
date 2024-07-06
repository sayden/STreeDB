package fss3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	db "github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

type s3FilesystemBuilder[T db.Entry] func(cfg *db.Config, s3cfg *aws.Config, client *s3.Client, rootPath string) db.Filesystem[T]

func newS3FilesystemParquet[T db.Entry](cfg *db.Config, s3cfg *aws.Config, client *s3.Client, rootPath string) db.Filesystem[T] {
	s3fs := &s3ParquetFs[T]{
		cfg:      cfg,
		s3cfg:    s3cfg,
		client:   client,
		rootPath: rootPath,
	}

	return s3fs
}

func newS3FilesystemJSON[T db.Entry](cfg *db.Config, s3cfg *aws.Config, client *s3.Client, rootPath string) db.Filesystem[T] {
	s3fs := &s3JSONFs[T]{
		cfg:      cfg,
		s3cfg:    s3cfg,
		client:   client,
		rootPath: rootPath,
	}

	return s3fs
}

func initS3[T db.Entry](cfg *db.Config, level int, builder s3FilesystemBuilder[T]) (db.Filesystem[T], error) {
	s3Cfg, err := s3config.LoadDefaultConfig(
		context.TODO(),
		s3config.WithRegion(cfg.S3Config.Region),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(s3Cfg, func(o *s3.Options) {
		// o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
		o.BaseEndpoint = aws.String("http://127.0.0.1:8080")
		o.UsePathStyle = true // S3ninja typically requires path-style addresing
	})

	rootPath := fmt.Sprintf("%02d", level)
	s3fs := builder(cfg, &s3Cfg, client, rootPath)

	return s3fs, nil
}

func openS3[T db.Entry](client *s3.Client, cfg *db.Config, p string, f db.Filesystem[T], listeners []db.FileblockListener[T]) (db.Fileblock[T], error) {
	out, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Key:    aws.String(p),
	})
	if err != nil {
		return nil, errors.Join(errors.New("open error getting obj from S3"), err)
	}
	defer out.Body.Close()

	meta := &db.MetaFile[T]{}
	if err = json.NewDecoder(out.Body).Decode(&meta); err != nil {
		return nil, errors.Join(errors.New("open error decoding metadata"), err)
	}
	block := db.NewFileblock(cfg, meta, f)

	for _, listener := range listeners {
		listener.OnNewFileblock(block)
	}

	return db.NewFileblock(cfg, meta, f), nil
}

func removeS3[T db.Entry](client *s3.Client, cfg *db.Config, fb db.Fileblock[T], listeners ...db.FileblockListener[T]) error {
	m := fb.Metadata()
	log.Debugf("Removing parquet block data in '%s'", m.DataFilepath)

	_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Key:    aws.String(m.DataFilepath),
	})
	if err != nil {
		log.WithError(err).Error("error deleting data file")
	}

	log.Debugf("Removing parquet block's meta in '%s'", m.MetaFilepath)

	if _, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Key:    aws.String(m.MetaFilepath),
	}); err != nil {
		log.WithError(err).Error("error deleting meta file")
	}

	for _, listener := range listeners {
		listener.OnFileblockRemoved(fb)
	}

	return nil
}

func openAllMetadataFilesInS3Folder[T db.Entry](cfg *db.Config, client *s3.Client, filesystem db.Filesystem[T], rootPath string, listeners ...db.FileblockListener[T]) error {
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Prefix: aws.String(rootPath + "/meta_"),
	}

	paginator := s3.NewListObjectsV2Paginator(client, listInput)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Errorf("error getting paginator to list objects in S3: %v\n", err)
			break
		}
		if page.KeyCount != nil {
			log.WithField("items", *page.KeyCount).Debug("Iterating page")
		}

		for _, object := range page.Contents {
			if _, err = openS3(client, cfg, *object.Key, filesystem, listeners); err != nil {
				return err
			}
		}
	}

	return nil
}

func updateMetadataS3[T db.Entry](cfg *db.Config, client *s3.Client, m *db.MetaFile[T]) error {
	byt, err := json.Marshal(m)
	if err != nil {
		return errors.Join(errors.New("error encoding entries"), err)
	}

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Key:    aws.String(m.MetaFilepath),
		Body:   bytes.NewReader(byt),
	})
	if err != nil {
		return errors.Join(errors.New("error updating obj to S3"), err)
	}

	return nil
}
