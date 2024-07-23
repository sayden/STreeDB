package main

import (
	"net/http"
	"time"

	"github.com/a-h/templ/examples/integration-gin/gintemplrenderer"
	"github.com/gin-gonic/gin"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/core"
	"github.com/sayden/streedb/metrics"
)

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
		for i := 0; i < 30; i++ {
			ts := make([]int64, 0, 10)
			vals := make([]int32, 0, 10)
			for i := 0; i < 10; i++ {
				ts = append(ts, time.Now().UnixMilli())
				vals = append(vals, int32(i))
				time.Sleep(time.Millisecond)
			}

			kv := db.NewKv("instance1", "cpu", ts, vals)
			if err = dbWrapper.Append(kv); err != nil {
				panic(err)
			}

			kv = db.NewKv("instance1", "mem", ts, vals)
			if err = dbWrapper.Append(kv); err != nil {
				panic(err)
			}

		}
	}()

	// Start the metrics server
	metricsServer := &ServerMetrics[int64, *db.Kv]{db: dbWrapper}

	router := gin.Default()
	ginHtmlRenderer := router.HTMLRender
	router.HTMLRender = &gintemplrenderer.HTMLTemplRenderer{
		FallbackHtmlRenderer: ginHtmlRenderer,
	}

	router.LoadHTMLGlob("static/*")

	router.GET("/templ", func(c *gin.Context) {
		em, found, err := metricsServer.getMetrics()
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		if !found {
			c.JSON(404, gin.H{"error": "metrics not found"})
			return
		}

		metric, ok := em["elapsed_nano"].(*metrics.MetricsEntry)
		if !ok {
			c.JSON(404, gin.H{"error": "metrics not found. Error extracting metrics entry"})
		}

		r := gintemplrenderer.New(c.Request.Context(), http.StatusOK, Index("Mario", metric.Ts, metric.Val))
		c.Render(http.StatusOK, r)
	})

	router.GET("/metrics", metricsServer.GETMetrics)
	router.GET("/ping", metricsServer.Ping)
	router.GET("/api/metrics", metricsServer.GETMetricsAPI)
	router.GET("/api/:pIdx", metricsServer.GETPrimaryAndSecondaryIndex)
	router.GET("/api/:pIdx/:sIdx", metricsServer.GETPrimaryAndSecondaryIndex)
	router.GET("/", metricsServer.GETIndex)

	router.Run()
}
