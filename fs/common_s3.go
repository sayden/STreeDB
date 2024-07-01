package fs

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

type s3FilesystemBuilder[T streedb.Entry] func(cfg *streedb.Config, s3cfg *aws.Config, client *s3.Client) streedb.Filesystem[T]

func newS3FilesystemParquet[T streedb.Entry](cfg *streedb.Config, s3cfg *aws.Config, client *s3.Client) streedb.Filesystem[T] {
	s3fs := &s3ParquetFs[T]{
		cfg:    cfg,
		s3cfg:  s3cfg,
		client: client,
	}

	return s3fs
}

func newS3FilesystemJSON[T streedb.Entry](cfg *streedb.Config, s3cfg *aws.Config, client *s3.Client) streedb.Filesystem[T] {
	s3fs := &s3JSONFs[T]{
		cfg:    cfg,
		s3cfg:  s3cfg,
		client: client,
	}

	return s3fs
}

func initS3[T streedb.Entry](cfg *streedb.Config, builder s3FilesystemBuilder[T]) (streedb.Filesystem[T], streedb.Levels[T], error) {
	s3Cfg, err := s3config.LoadDefaultConfig(
		context.TODO(),
		s3config.WithRegion(cfg.S3Config.Region),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		return nil, nil, err
	}

	client := s3.NewFromConfig(s3Cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8080")
		o.UsePathStyle = true // S3ninja typically requires path-style addresing
	})

	s3fs := builder(cfg, &s3Cfg, client)
	levels, err := s3fs.OpenAllMetaFiles()
	if err != nil {
		return nil, nil, errors.Join(errors.New("error loading metadata from storage"), err)
	}

	return s3fs, levels, nil
}

func openS3[T streedb.Entry](client *s3.Client, cfg *streedb.Config, p string) (*streedb.MetaFile[T], error) {
	out, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Key:    aws.String(p),
	})
	if err != nil {
		return nil, errors.Join(errors.New("Open error getting obj from S3"), err)
	}
	defer out.Body.Close()

	meta := &streedb.MetaFile[T]{}
	if err = json.NewDecoder(out.Body).Decode(&meta); err != nil {
		return nil, errors.Join(errors.New("Open error decoding metadata"), err)
	}

	return meta, nil
}

func removeS3[T streedb.Entry](client *s3.Client, cfg *streedb.Config, m *streedb.MetaFile[T]) error {
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

	return nil
}

type s3FileblockBuilder[T streedb.Entry] func(cfg *streedb.Config, meta *streedb.MetaFile[T], fs streedb.Filesystem[T]) streedb.Fileblock[T]

func newS3FileblockParquet[T streedb.Entry](cfg *streedb.Config, meta *streedb.MetaFile[T], fs streedb.Filesystem[T]) streedb.Fileblock[T] {
	return &s3ParquetFileblock[T]{
		MetaFile: *meta,
		fs:       fs,
		cfg:      cfg,
	}

}

func newS3FileblockJSON[T streedb.Entry](cfg *streedb.Config, meta *streedb.MetaFile[T], fs streedb.Filesystem[T]) streedb.Fileblock[T] {
	return &s3JSONFileblock[T]{
		MetaFile: *meta,
		fs:       fs,
		cfg:      cfg,
	}
}

func openAllMetadataFilesInS3[T streedb.Entry](cfg *streedb.Config, client *s3.Client, fs streedb.Filesystem[T], builder s3FileblockBuilder[T]) (streedb.Levels[T], error) {
	levels := streedb.NewLevels[T](cfg, fs)

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Prefix: aws.String("meta_"),
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
			meta, err := fs.Open(*object.Key)
			if err != nil {
				return nil, errors.Join(errors.New("error opening meta file"), err)
			}

			log.WithFields(log.Fields{"items": meta.ItemCount, "min": meta.Min, "max": meta.Max}).Debugf("Opened meta file '%s'", *object.Key)

			fileblock := builder(cfg, meta, fs)

			levels.AppendFile(fileblock)
		}
	}

	return levels, nil
}
