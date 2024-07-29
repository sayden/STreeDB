package main

import (
	"strings"
	"time"

	rmetrics "runtime/metrics"

	"github.com/gin-gonic/gin"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/core"
	"github.com/sayden/streedb/metrics"
)

func cleanSampleName(s string) string {
	s = strings.ReplaceAll(s, "/", " ")
	s = strings.ReplaceAll(s, ":", " ")
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")

	return s
}

func main() {
	cfg := db.NewDefaultConfig()
	coreDb, err := core.NewLsmTree[int64, *db.Kv](cfg)
	if err != nil {
		panic(err)
	}
	defer coreDb.Close()

	dbWrapper, err := metrics.New[int64, *db.Kv](coreDb)
	if err != nil {
		panic(err)
	}
	defer dbWrapper.Close()

	go func() {
		for {
			// Create a slice to hold the metrics you want to read
			samples := []rmetrics.Sample{
				{Name: "/memory/classes/heap/free:bytes"},
				{Name: "/gc/cycles/total:gc-cycles"},
				{Name: "/cpu/classes/total:cpu-seconds"},
				{Name: "/memory/classes/heap/objects:objects"},
				{Name: "/memory/classes/heap/alloc:bytes"},
				{Name: "/sched/goroutines:goroutines"},
			}

			// Read the metrics
			rmetrics.Read(samples)
			// metricsDesc := rmetrics.All()
			// metricsDesc[0].Name

			// Process the results
			for _, sample := range samples {
				switch sample.Value.Kind() {
				case rmetrics.KindUint64:
					metric := metrics.NewMetric("go_perf", cleanSampleName(sample.Name), time.Now().UnixMilli(), float64(sample.Value.Uint64()))
					if err = dbWrapper.Metrics.Append(metric); err != nil {
						panic(err)
					}
				case rmetrics.KindFloat64:
					metric := metrics.NewMetric("go_perf", cleanSampleName(sample.Name), time.Now().UnixMilli(), sample.Value.Float64())
					if err = dbWrapper.Metrics.Append(metric); err != nil {
						panic(err)
					}
				}
			}

			// time.Sleep(30 * time.Second)
			time.Sleep(time.Second)
			// time.Sleep(time.Millisecond)
		}
	}()

	// Start the metrics server
	metricsServer := &ServerMetrics[int64, *db.Kv]{db: dbWrapper}

	router := gin.Default()

	router.GET("/ping", metricsServer.Ping)
	router.GET("/api/metrics", metricsServer.GETMetricsAPI)
	router.GET("/api/metrics/:pIdx", metricsServer.GETMetricsAPI)
	router.GET("/api/metrics/:pIdx/:sIdx", metricsServer.GETMetricsAPI)
	router.GET("/api/:pIdx", metricsServer.GETPrimaryAndSecondaryIndex)
	router.GET("/api/:pIdx/:sIdx", metricsServer.GETPrimaryAndSecondaryIndex)

	router.Run()
}
