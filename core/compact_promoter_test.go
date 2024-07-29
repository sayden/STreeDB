package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewItemLimitPromoter(t *testing.T) {
	cfg := db.NewDefaultConfig()
	cfg.Compaction.Promoters.ItemLimit = db.ItemLimitPromoterCfg{
		GrowthFactor:        8,
		FirstBlockItemCount: 512,
		MaxItems:            1024 * 8 * 8 * 5,
	}

	promoter := newItemLimitPromoter[int64](cfg)
	concretePromoter := promoter.(*itemLimitPromoter[int64])

	require.Equal(t, int64(512), concretePromoter.blockSizes[0])
	require.Equal(t, int64(512*8), concretePromoter.blockSizes[1])
	require.Equal(t, int64(512*8*8), concretePromoter.blockSizes[2])
	require.Equal(t, int64(512*8*8*8), concretePromoter.blockSizes[3])
	require.Equal(t, int64(cfg.Compaction.Promoters.ItemLimit.MaxItems), concretePromoter.blockSizes[4])

	testBuilders := []struct {
		builder       *db.MetadataBuilder[int64]
		expectedLevel int
	}{
		{builder: db.NewMetadataBuilder[int64](cfg).WithItemCount(10), expectedLevel: 0},
		{builder: db.NewMetadataBuilder[int64](cfg).WithItemCount(513), expectedLevel: 1},
		{builder: db.NewMetadataBuilder[int64](cfg).WithItemCount(1024), expectedLevel: 1},
		{builder: db.NewMetadataBuilder[int64](cfg).WithItemCount(1024 * 8 * 8 * 5), expectedLevel: 4},
		{builder: db.NewMetadataBuilder[int64](cfg).WithItemCount(1024 * 8 * 8 * 15), expectedLevel: 4},
	}

	for _, test := range testBuilders {
		err := promoter.Promote(test.builder)
		assert.Equal(t, test.expectedLevel, test.builder.GetLevel())
		if err != nil {
			t.Errorf("Promote failed: %v", err)
		}
	}
}
