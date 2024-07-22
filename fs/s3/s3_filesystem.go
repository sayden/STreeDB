package fss3

import (
	"cmp"
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

func initS3[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, level int) (db.Filesystem[O], error) {
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
	s3fs := s3ParquetFs[O, E]{cfg, &s3Cfg, client, rootPath}

	return &s3fs, nil
}

func openS3[O cmp.Ordered](client *s3.Client, cfg *db.Config, p string, f db.Filesystem[O], listeners []db.FileblockListener[O]) (*db.Fileblock[O], error) {
	out, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(cfg.S3Config.Bucket),
		Key:    aws.String(p),
	})
	if err != nil {
		return nil, errors.Join(errors.New("open error getting obj from S3"), err)
	}
	defer out.Body.Close()

	meta := &db.MetaFile[O]{}
	if err = json.NewDecoder(out.Body).Decode(&meta); err != nil {
		return nil, errors.Join(errors.New("open error decoding metadata"), err)
	}
	block := db.NewFileblock(cfg, meta, f)

	for _, listener := range listeners {
		listener.OnFileblockCreated(block)
	}

	return db.NewFileblock(cfg, meta, f), nil
}

func openAllMetadataFilesInS3Folder[O cmp.Ordered](cfg *db.Config, client *s3.Client, filesystem db.Filesystem[O], rootPath string, listeners ...db.FileblockListener[O]) error {
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
