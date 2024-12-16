package management

import (
	"sync/atomic"
	"time"
)

type BulkMetrics struct {
	OperationsProcessed atomic.Int64
	BatchesProcessed    atomic.Int64
	AverageLatency      atomic.Value // stores time.Duration
	ErrorCount          atomic.Int64
	LastProcessedTime   atomic.Value // stores time.Time
}

func NewBulkMetrics() *BulkMetrics {
	metrics := &BulkMetrics{}
	metrics.AverageLatency.Store(time.Duration(0))
	metrics.LastProcessedTime.Store(time.Now())
	return metrics
}
