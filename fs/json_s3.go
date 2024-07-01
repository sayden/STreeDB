package fs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/sayden/streedb"
	"github.com/thehivecorporation/log"
)

func InitJSONS3[T streedb.Entry](cfg *streedb.Config) (streedb.Filesystem[T], streedb.Levels[T], error) {
	return initS3[T](cfg, newS3FilesystemJSON)
}

type s3JSONFs[T streedb.Entry] struct {
	cfg      *streedb.Config
	s3cfg    s3config.Config
	client   *s3.Client
	rootPath string
}

func (s *s3JSONFs[T]) Open(p string) (*streedb.MetaFile[T], error) {
	return openS3[T](s.client, s.cfg, p)
}

func (f *s3JSONFs[T]) Load(m *streedb.MetaFile[T]) (streedb.Entries[T], error) {
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
	entries := make(streedb.Entries[T], 0, m.ItemCount)
	if err = json.NewDecoder(out.Body).Decode(&entries); err != nil {
		return nil, errors.Join(errors.New("Load error decoding entries"), err)
	}

	return entries, nil
}

func (f *s3JSONFs[T]) Merge(a, b streedb.Fileblock[T]) (streedb.Fileblock[T], error) {
	newEntries, err := merge(a, b)
	if err != nil {
		return nil, err
	}
	return f.Create(newEntries, a.Metadata().Level)
}

func (f *s3JSONFs[T]) Create(entries streedb.Entries[T], level int) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T]("").
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".jsondata").
		Build()
	if err != nil {
		return nil, errors.Join(errors.New("error creating metadata"), err)
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

	return &s3JSONFileblock[T]{MetaFile: *meta, fs: f}, nil
}

func (f *s3JSONFs[T]) Remove(m *streedb.MetaFile[T]) error {
	return removeS3[T](f.client, f.cfg, m)
}

func (f *s3JSONFs[T]) OpenAllMetaFiles() (streedb.Levels[T], error) {
	return openAllMetadataFilesInS3[T](f.cfg, f.client, f, newS3FileblockJSON)
}

// s3JSONFileblock works using plain JSON files to store data (and metadata).
type s3JSONFileblock[T streedb.Entry] struct {
	streedb.MetaFile[T]
	cfg *streedb.Config
	fs  streedb.Filesystem[T]
}

func (l *s3JSONFileblock[T]) Load() (streedb.Entries[T], error) {
	return l.fs.Load(&l.MetaFile)
}

func (l *s3JSONFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInsideMinMax(l.Min, l.Max, v) {
		return nil, false, nil
	}

	entries, err := l.Load()
	if err != nil {
		return nil, false, err
	}

	entry, found := entries.Find(v)

	return entry, found, nil
}

func (l *s3JSONFileblock[T]) Metadata() *streedb.MetaFile[T] {
	return &l.MetaFile
}

func (l *s3JSONFileblock[T]) Close() error {
	//noop
	return nil
}

// newJSONFileblock is used to create new JSON files.
// `entries` must contain the data to be written to the file.
// `level` is the destination level for the filebeock.
func newJSONS3Fileblock[T streedb.Entry](entries streedb.Entries[T], cfg *streedb.Config, level int, fs streedb.Filesystem[T]) (streedb.Fileblock[T], error) {
	if entries.Len() == 0 {
		return nil, errors.New("empty data")
	}

	meta, err := streedb.NewMetadataBuilder[T]("").
		WithEntries(entries).
		WithLevel(level).
		WithExtension(".jsondata").
		Build()
	if err != nil {
		return nil, err
	}

	return &s3JSONFileblock[T]{
		MetaFile: *meta,
		cfg:      cfg,
		fs:       fs,
	}, nil
}
