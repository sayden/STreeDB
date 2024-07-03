package fss3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	db "github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

func InitJSONS3[T db.Entry](cfg *db.Config, level int) (db.Filesystem[T], error) {
	return initS3[T](cfg, level, newS3FilesystemJSON)
}

type s3JSONFs[T db.Entry] struct {
	cfg      *db.Config
	s3cfg    s3config.Config
	client   *s3.Client
	rootPath string
}

func (s *s3JSONFs[T]) Open(p string) (*db.MetaFile[T], error) {
	return openS3[T](s.client, s.cfg, p)
}

func (f *s3JSONFs[T]) Load(b db.Fileblock[T]) (db.Entries[T], error) {
	m := b.Metadata()
	log.WithField("data_filepath", m.DataFilepath).Debug("Loading data from S3")

	out, err := f.client.GetObject(context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(f.cfg.S3Config.Bucket),
			Key:    aws.String(m.DataFilepath),
		})
	if err != nil {
		return nil, errors.Join(errors.New("Load error getting obj from S3"), err)
	}

	log.WithFields(log.Fields{"items": m.ItemCount, "min": m.Min, "max": m.Max}).Debugf("Opened data file '%s'", m.DataFilepath)
	entries := make(db.Entries[T], 0, m.ItemCount)
	if err = json.NewDecoder(out.Body).Decode(&entries); err != nil {
		return nil, errors.Join(errors.New("Load error decoding entries"), err)
	}

	return entries, nil
}

func (f *s3JSONFs[T]) UpdateMetadata(b db.Fileblock[T]) error {
	return updateMetadataS3(f.cfg, f.client, f, b.Metadata())
}

func (f *s3JSONFs[T]) Create(cfg *db.Config, entries db.Entries[T], meta *db.MetaFile[T]) (db.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	byt, err := json.Marshal(entries)
	if err != nil {
		return nil, errors.Join(errors.New("error encoding entries"), err)
	}

	// data file
	if _, err = f.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(f.cfg.S3Config.Bucket),
		Key:    aws.String(meta.DataFilepath),
		Body:   bytes.NewReader(byt),
	}); err != nil {
		return nil, errors.Join(errors.New("error putting obj to S3"), err)
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

	if byt, err = json.Marshal(meta); err != nil {
		return nil, errors.Join(errors.New("error encoding metadata"), err)
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

	log.Debug("Created new JSON fileblock in S3")

	return NewS3Fileblock(cfg, meta, f), nil
}

func (f *s3JSONFs[T]) Remove(b db.Fileblock[T]) error {
	return removeS3(f.client, f.cfg, b.Metadata())
}

func (f *s3JSONFs[T]) OpenAllMetaFiles() (db.Levels[T], error) {
	return openAllMetadataFilesInS3(f.cfg, f.client, f, f.rootPath)
}

func (f *s3JSONFs[T]) OpenMetaFilesInLevel(level db.Level[T]) error {
	return openAllMetadataFilesInS3Folder(f.cfg, f.client, f, f.rootPath, level)
}

func (f *s3JSONFs[T]) FillMetadataBuilder(meta *db.MetadataBuilder[T]) *db.MetadataBuilder[T] {
	return meta.WithRootPath(f.rootPath).WithExtension(".jsondata")
}
