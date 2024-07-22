package fsmemory

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryFsBasic(t *testing.T) {
	cfg := db.NewDefaultConfig()
	fbc := NewMemoryFs[int64](cfg)
	require.NotNil(t, fbc)

	em := db.NewEntriesMap[int64]()
	em.Append(db.NewKv("idx", "key", []int64{1, 2, 3}, []int32{1, 2, 3}))
	fb, err := fbc.Create(cfg, em, db.NewMetadataBuilder[int64](cfg), nil)
	require.NoError(t, err)
	require.NotNil(t, fb)

	assert.NotEmpty(t, fb.Uuid)
}
