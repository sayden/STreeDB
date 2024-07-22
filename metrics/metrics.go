package metrics

import (
	"cmp"
	"path"
	"time"

	"github.com/rs/zerolog/log"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/core"
)

func New[O cmp.Ordered, E db.Entry[O]](ops db.LsmTreeOps[O, E]) (*Metrics[O, E], error) {
	cfg := db.NewDefaultConfig()
	cfg.LevelFilesystems = nil
	cfg.MaxLevels = 3
	cfg.Filesystem = "memory"
	cfg.DbPath = path.Join(cfg.DbPath, "metrics")
	cfg.MaxLevels = 1

	metrics, err := core.NewLsmTree[int64, *MetricsEntry](cfg)
	if err != nil {
		return nil, err
	}

	return &Metrics[O, E]{
		db:      ops,
		Metrics: metrics,
	}, nil
}

type Metrics[O cmp.Ordered, E db.Entry[O]] struct {
	db      db.LsmTreeOps[O, E]
	Metrics db.LsmTreeOps[int64, *MetricsEntry]
}

func (m *Metrics[O, E]) Append(d db.Entry[O]) error {
	now := time.Now()
	defer func() {
		elapsed := time.Since(now)
		log.Debug().Fields(map[string]interface{}{
			"elapsed":      elapsed,
			"primaryIdx":   d.PrimaryIndex(),
			"secondaryIdx": d.SecondaryIndex()}).
			Msg("Append")

		if err := m.Metrics.Append(
			NewMetric("append", "elapsed_micro", time.Now().UnixMilli(), elapsed.Microseconds())); err != nil {
			log.Err(err).Msg("Failed to append metric")
		}
	}()

	return m.db.Append(d)
}

func (m *Metrics[O, E]) Find(pIdx, sIdx string, min, max O) (db.EntryIterator[O], bool, error) {
	now := time.Now()
	defer func() {
		elapsed := time.Since(now)
		log.Debug().Fields(map[string]interface{}{
			"elapsed":      elapsed,
			"primaryIdx":   pIdx,
			"secondaryIdx": sIdx}).
			Msg("Find")
		if err := m.Metrics.Append(NewMetric("find", "elapsed_micro", time.Now().UnixMilli(), elapsed.Microseconds())); err != nil {
			log.Err(err).Msg("Failed to append metric")
		}
	}()

	return m.db.Find(pIdx, sIdx, min, max)
}

func (m *Metrics[O, E]) Close() error {
	now := time.Now()
	defer func() {
		elapsed := time.Since(now)
		log.Debug().Str("elapsed", elapsed.String()).Msg("Close")
		if err := m.Metrics.Append(NewMetric("close", "elapsed_micro", time.Now().UnixMilli(), elapsed.Microseconds())); err != nil {
			log.Err(err).Msg("Failed to append metric")
		}
		m.Metrics.Close()
	}()

	err := m.db.Close()

	if err != nil {
		return err
	}

	return nil
}

func (m *Metrics[O, E]) Compact() error {
	now := time.Now()
	defer func() {
		elapsed := time.Since(now)
		log.Debug().Str("elapsed", elapsed.String()).Msg("Compact")
		if err := m.Metrics.Append(NewMetric("compact", "elapsed_micro", time.Now().UnixMilli(), elapsed.Microseconds())); err != nil {
			log.Err(err).Msg("Failed to append metric")
		}
	}()

	return m.db.Compact()
}

func (m *Metrics[O, E]) GetMetrics() (db.EntryIterator[int64], bool, error) {
	return m.Metrics.Find("", "", 0, time.Now().UnixMilli()+10000)
}
