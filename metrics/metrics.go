package metrics

import (
	"cmp"
	"path"
	"time"

	"github.com/rs/zerolog/log"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/core"
)

func New[O cmp.Ordered, E db.Entry[O]](ops db.LsmTreeOps[O, E]) (db.LsmTreeOps[O, E], error) {
	cfg := db.NewDefaultConfig()
	cfg.DbPath = path.Join(cfg.DbPath, "metrics")
	cfg.Filesystem = "local"
	cfg.LevelFilesystems = []string{"local"}
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
		log.Debug().
			Fields(map[string]interface{}{
				"elapsed":      elapsed,
				"primaryIdx":   d.PrimaryIndex(),
				"secondaryIdx": d.SecondaryIndex()}).
			Msg("Append")
		if err := m.Metrics.Append(NewMetric("elapsed_ms", "append", time.Now().UnixMilli(), elapsed.Milliseconds())); err != nil {
			log.Err(err).Msg("Failed to append metric")
		}
	}()

	return m.db.Append(d)
}

func (m *Metrics[O, E]) Find(pIdx, sIdx string, min, max O) (db.EntryIterator[O], bool, error) {
	now := time.Now()
	defer func() {
		log.Debug().
			Fields(map[string]interface{}{
				"elapsed":      time.Since(now),
				"primaryIdx":   pIdx,
				"secondaryIdx": sIdx}).
			Msg("Find")
		if err := m.Metrics.Append(NewMetric("elapsed_ms", "find", time.Now().UnixMilli(), 1)); err != nil {
			log.Err(err).Msg("Failed to append metric")
		}
	}()

	return m.db.Find(pIdx, sIdx, min, max)
}

func (m *Metrics[O, E]) Close() error {
	now := time.Now()
	defer func() {
		log.Debug().Str("elapsed", time.Since(now).String()).Msg("Close")
		if err := m.Metrics.Append(NewMetric("elapsed_ms", "close", time.Now().UnixMilli(), 1)); err != nil {
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
		log.Debug().Str("elapsed", time.Since(now).String()).Msg("Compact")
		if err := m.Metrics.Append(NewMetric("elapsed_ms", "compact", time.Now().UnixMilli(), 1)); err != nil {
			log.Err(err).Msg("Failed to append metric")
		}
	}()

	return m.db.Compact()
}
