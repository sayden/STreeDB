package streedb

import (
	"cmp"
	"time"
)

type MetaFile[O cmp.Ordered] struct {
	CreatedAt  time.Time
	ItemCount  int
	Size       int64
	Level      int
	Uuid       string
	PrimaryIdx string
	Min        *O
	Max        *O
	Rows       []Row[O]

	DataFilepath string `json:"Datafile"`
	MetaFilepath string `json:"Metafile"`
}

type Row[O cmp.Ordered] struct {
	SecondaryIdx string
	ItemCount    int
	Min          O
	Max          O
}

func (r *Row[O]) Merge(o Comparable[O]) {
	if r.SecondaryIdx == "" {
		r.SecondaryIdx = o.SecondaryIndex()
	}

	r.ItemCount += o.Len()
	if o.Min() < r.Min {
		r.Min = o.Min()
	}

	if o.Max() > r.Max {
		r.Max = o.Max()
	}
}

func (m *MetaFile[O]) MaxAtSecondary(s string) (O, bool) {
	for _, rg := range m.Rows {
		if rg.SecondaryIdx == s {
			return rg.Max, true
		}
	}

	return (*new(O)), false
}

func (m *MetaFile[O]) MinAtSecondary(s string) (O, bool) {
	for _, rg := range m.Rows {
		if rg.SecondaryIdx == s {
			return rg.Min, true
		}
	}

	return (*new(O)), false
}

func (m *MetaFile[O]) Metadata() *MetaFile[O] {
	return m
}

func (m *MetaFile[O]) UUID() string {
	return m.Uuid
}
