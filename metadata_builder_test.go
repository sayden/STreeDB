package streedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadataBuilder(t *testing.T) {
	// Create a new metadata builder
	es := NewKv("key1", "pidx1", []int32{1, 2, 3})
	meta, err := NewMetadataBuilder[int32](&Config{MaxLevels: 5}).
		WithEntry(es).
		WithLevel(1).
		WithFilenamePrefix("01").
		WithRootPath("/tmp/db/json").
		WithExtension("ext").
		Build()
	assert.NoError(t, err)

	// Check the metadata
	assert.Equal(t, 1, meta.Level)
	assert.Equal(t, 3, meta.ItemCount)
	assert.Contains(t, meta.DataFilepath, "/tmp/db/json/01")
	assert.Contains(t, meta.DataFilepath, "ext")
	assert.Contains(t, meta.MetaFilepath, "/tmp/db/json/01")
	assert.Contains(t, meta.MetaFilepath, ".json")

	min := *meta.Min
	max := *meta.Max
	assert.Equal(t, int32(1), min)
	assert.Equal(t, int32(3), max)
}
