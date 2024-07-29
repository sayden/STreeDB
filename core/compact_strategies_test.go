package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func TestSamePrimaryIndex(t *testing.T) {
	t.Skip()
}

func TestOverlapping(t *testing.T) {
	st := overlappingCompactionStrategy[int64]{}

	a, b := &db.MetaFile[int64]{
		PrimaryIdx: "a",
		Rows: []db.Row[int64]{{
			SecondaryIdx: "b",
			Min:          1,
			Max:          2,
		}},
	}, &db.MetaFile[int64]{
		PrimaryIdx: "a",
		Rows: []db.Row[int64]{{
			SecondaryIdx: "b",
			Min:          3,
			Max:          5,
		}},
	}

	shouldMerge := st.ShouldMerge(a, b)
	assert.True(t, shouldMerge)

	b.Rows[0].Min = 4
	shouldMerge = st.ShouldMerge(a, b)
	assert.False(t, shouldMerge)
}

func TestOr(t *testing.T) {
	t.Skip()
}
