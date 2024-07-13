package fslocal

import (
	"os"
	"testing"
	"time"

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
	os.MkdirAll(tmpDir, os.ModePerm)

	now := time.Now()
	t.Cleanup(func() {
		t.Logf("%s: %v", t.Name(), time.Since(now))
		t.Logf("File written in %s\n", tmpDir+"/data1.parquet")
	})

	dataFile, err := os.Create(tmpDir + "/data1.parquet")
	require.NoError(t, err)
	defer dataFile.Close()

	parquetWriter, err := writer.NewParquetWriterFromWriter(dataFile, new(db.Kv), 4)
	require.NoError(t, err)

	ints := make([]int32, 0, 2000000)
	for i := 0; i < 2000000; i++ {
		ints = append(ints, int32(i))
	}

	err = parquetWriter.Write(db.Kv{Key: "key", Val: ints, PrimaryIdx: "idx"})
	require.NoError(t, err)
	err = parquetWriter.WriteStop()
	require.NoError(t, err)

	pf, err := local.NewLocalFileReader(dataFile.Name())
	require.NoError(t, err)
	defer pf.Close()

	pr, err := reader.NewParquetReader(pf, nil, 4)
	require.NoError(t, err)

	numRows := int(pr.GetNumRows())
	// entries := db.NewEntriesSlice[int32, *db.Kv](numRows)
	entries := make([]db.Kv, 0, numRows)
	err = pr.Read(&entries)
	require.NoError(t, err)
}

func TestParquetLocalFilesystem(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("/tmp/db")
	})

	cfg := db.NewDefaultConfig()
	fsp, err := InitParquetLocal[int32, *db.Kv](cfg, 0)
	require.NoError(t, err)

	temp := make([]*db.Kv, 0)
	data := db.NewSliceToMap(temp)
	n := 2000
	ints := make([]int32, 0, n)
	for i := 0; i < n; i++ {
		ints = append(ints, int32(i))
	}
	data.Append(db.NewKv("key", "idx", ints))
	data.Append(db.NewKv("key2", "idx", ints))
	builder := db.NewMetadataBuilder[int32](cfg)
	builder.WithEntry(data.Get("key"))
	builder.WithEntry(data.Get("key2"))

	var fb *db.Fileblock[int32, *db.Kv]
	t.Run("Create", func(t *testing.T) {
		fb, err = fsp.Create(cfg, data, builder, nil)
		require.NoError(t, err)

		assert.NotEmpty(t, fb.DataFilepath)
		assert.Contains(t, fb.DataFilepath, ".parquet")
		assert.NotEmpty(t, fb.MetaFilepath)
		assert.Contains(t, fb.MetaFilepath, ".json")

		assert.Equal(t, 2*n, fb.ItemCount)
		assert.Equal(t, int64(621), fb.Size)
		assert.Equal(t, int32(0), *fb.Min)
		assert.Equal(t, int32(n-1), *fb.Max)
		for _, row := range fb.Rows {
			assert.Equal(t, int32(0), row.Min)
			assert.Equal(t, int32(n-1), row.Max)
			assert.Equal(t, n, row.ItemCount)
		}
	})

	t.Run("Load", func(t *testing.T) {
		es, err := fsp.Load(fb)
		require.NoError(t, err)
		entries, ok := es.(db.EntriesMap[int32, *db.Kv])
		require.True(t, ok)

		assert.Equal(t, 2, entries.SecondaryIndices())
		assert.Equal(t, "key", entries.Get("key").Key)
		assert.Equal(t, "key2", entries.Get("key2").Key)
		assert.Equal(t, 2000, len(entries.Get("key").Val))
		assert.Equal(t, 2000, len(entries.Get("key2").Val))
	})

	t.Run("OpenMetaFilesInLevel", func(t *testing.T) {
		listener := &testFileblockListener{}
		err := fsp.OpenMetaFilesInLevel([]db.FileblockListener[int32, *db.Kv]{listener})
		require.NoError(t, err)
		assert.Equal(t, 1, listener.created)
		assert.Equal(t, 0, listener.removed)
	})

	t.Run("Remove", func(t *testing.T) {
		listener := &testFileblockListener{}
		err := fsp.Remove(fb, []db.FileblockListener[int32, *db.Kv]{listener})
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

func (l *testFileblockListener) OnFileblockCreated(fb *db.Fileblock[int32, *db.Kv]) {
	l.created++
}

func (l *testFileblockListener) OnFileblockRemoved(fb *db.Fileblock[int32, *db.Kv]) {
	l.removed++
}
