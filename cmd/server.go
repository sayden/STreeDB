package main

import (
	"cmp"
	"net/http"

	"github.com/gin-gonic/gin"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/metrics"
)

type ServerMetrics[O cmp.Ordered, E db.Entry[O]] struct {
	db *metrics.Metrics[O, E]
}

func (s *ServerMetrics[_, _]) GetMetrics(c *gin.Context) {
	// Get the iter
	iter, found, err := s.db.GetMetrics()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "metrics not found"})
		return
	}

	// Accumulate the metrics using the iterator
	em := db.NewEntriesMap[int64]()
	for entry, found, err := iter.Next(); entry != nil && found && err == nil; entry, found, err = iter.Next() {
		em.Append(entry)
	}

	// Return the metrics
	c.JSON(http.StatusOK, em)
}
