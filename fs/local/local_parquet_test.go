package fslocal

import (
	"os"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func TestParquetFiles(t *testing.T) {
	tmpDir := t.TempDir()
	// tmpDir := os.TempDir() + "/parquet"
	// os.MkdirAll(tmpDir, os.ModePerm)

	// t.Cleanup(func() { os.RemoveAll(tmpDir) })

	dataFile, err := os.Create(tmpDir + "/data1.parquet")
	require.NoError(t, err)
	require.NotNil(t, dataFile)
	defer dataFile.Close()

	parquetWriter, err := writer.NewParquetWriterFromWriter(dataFile, new(db.Kv), 4)
	require.NoError(t, err)

	ints := make([]int32, 0, 200000)
	ts := make([]int64, 0, 200000)
	for i := 0; i < 200000; i++ {
		ts = append(ts, int64(i))
		ints = append(ints, int32(i))
	}

	err = parquetWriter.Write(db.NewKv("idx", "key", ts, ints))
	require.NoError(t, err)
	err = parquetWriter.WriteStop()
	require.NoError(t, err)

	pf, err := local.NewLocalFileReader(dataFile.Name())
	require.NoError(t, err)
	defer pf.Close()

	pr, err := reader.NewParquetReader(pf, nil, 4)
	require.NoError(t, err)

	numRows := int(pr.GetNumRows())
	entries := make([]db.Kv, 0, numRows)
	err = pr.Read(&entries)
	require.NoError(t, err)
}

func TestParquetLocalFilesystem(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("/tmp/db")
	})

	cfg := db.NewDefaultConfig()
	fsp, err := InitParquetLocal[int64, *db.Kv](cfg, 0)
	require.NoError(t, err)
	require.NotNil(t, fsp)

	entriesMap := db.NewEntriesMap[int64]()
	n := 2000
	ints := make([]int32, 0, n)
	ts := make([]int64, n)
	for i := 0; i < n; i++ {
		ts[i] = int64(i)
		ints = append(ints, int32(i))
	}
	entriesMap.Append(db.NewKv("idx", "key", ts, ints))
	entriesMap.Append(db.NewKv("idx", "key2", ts, ints))
	builder := db.NewMetadataBuilder[int64](cfg)

	key := entriesMap.Get("key")
	require.NotNil(t, key)
	key2 := entriesMap.Get("key2")
	require.NotNil(t, key2)

	builder.WithEntry(key)
	builder.WithEntry(key2)

	var fb *db.Fileblock[int64]

	t.Run("Create", func(t *testing.T) {
		fb, err = fsp.Create(cfg, entriesMap, builder, nil)
		require.NoError(t, err)
		require.NotNil(t, fb)

		assert.NotEmpty(t, fb.DataFilepath)
		assert.Contains(t, fb.DataFilepath, ".parquet")
		assert.NotEmpty(t, fb.MetaFilepath)
		assert.Contains(t, fb.MetaFilepath, ".json")

		assert.Equal(t, 2*n, fb.ItemCount)
		assert.Equal(t, int64(1213), fb.Size)
		assert.Equal(t, int64(0), *fb.Min)
		assert.Equal(t, int64(n-1), *fb.Max)

		for _, row := range fb.Rows {
			assert.Equal(t, int64(0), row.Min)
			assert.Equal(t, int64(n-1), row.Max)
			assert.Equal(t, n, row.ItemCount)
		}
	})

	require.NotNil(t, fb)
	require.NotNil(t, fsp)

	t.Run("Load", func(t *testing.T) {
		entries, err := fsp.Load(fb)
		require.NoError(t, err)
		require.NotNil(t, entries)

		assert.Equal(t, 2, entries.SecondaryIndicesLen())
		assert.Equal(t, "key", entries.Get("key").SecondaryIndex())
		assert.Equal(t, "key2", entries.Get("key2").SecondaryIndex())
		kv := entries.Get("key").(*db.Kv)
		assert.Equal(t, 2000, len(kv.Ts))
		assert.Equal(t, 2000, len(kv.Val))
	})

	t.Run("OpenMetaFilesInLevel", func(t *testing.T) {
		listener := &testFileblockListener{}
		err := fsp.OpenMetaFilesInLevel([]db.FileblockListener[int64]{listener})
		require.NoError(t, err)
		assert.Equal(t, 1, listener.created)
		assert.Equal(t, 0, listener.removed)
	})

	t.Run("Remove", func(t *testing.T) {
		listener := &testFileblockListener{}
		err := fsp.Remove(fb, []db.FileblockListener[int64]{listener})
		require.NoError(t, err)
		assert.NoFileExists(t, fb.DataFilepath)
		assert.NoFileExists(t, fb.MetaFilepath)
		assert.Equal(t, 0, listener.created)
		assert.Equal(t, 1, listener.removed)
	})
}

type testFileblockListener struct {
	created, removed int
}

func (l *testFileblockListener) OnFileblockCreated(fb *db.Fileblock[int64]) {
	l.created++
}

func (l *testFileblockListener) OnFileblockRemoved(fb *db.Fileblock[int64]) {
	l.removed++
}
