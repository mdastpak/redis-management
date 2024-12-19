package timeout

import (
	"encoding/json"
	"sync/atomic"
	"time"
)

// Metrics holds timeout-related metrics
type Metrics struct {
	TimeoutCount       atomic.Int64             `json:"timeout_count"`
	SuccessCount       atomic.Int64             `json:"success_count"`
	TotalOperations    atomic.Int64             `json:"total_operations"`
	AverageLatency     atomic.Value             `json:"average_latency"` // stores time.Duration
	LatencyPercentiles map[string]time.Duration `json:"latency_percentiles"`
	LastUpdated        atomic.Value             `json:"last_updated"` // stores time.Time
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	m := &Metrics{
		LatencyPercentiles: make(map[string]time.Duration),
	}
	m.AverageLatency.Store(time.Duration(0))
	m.LastUpdated.Store(time.Now())
	return m
}

// RecordTimeout records a timeout occurrence
func (m *Metrics) RecordTimeout() {
	m.TimeoutCount.Add(1)
	m.TotalOperations.Add(1)
	m.LastUpdated.Store(time.Now())
}

// RecordSuccess records a successful operation
func (m *Metrics) RecordSuccess(latency time.Duration) {
	m.SuccessCount.Add(1)
	m.TotalOperations.Add(1)
	m.updateLatency(latency)
	m.LastUpdated.Store(time.Now())
}

// GetTimeoutRate returns the current timeout rate
func (m *Metrics) GetTimeoutRate() float64 {
	total := m.TotalOperations.Load()
	if total == 0 {
		return 0
	}
	return float64(m.TimeoutCount.Load()) / float64(total)
}

// updateLatency updates latency metrics
func (m *Metrics) updateLatency(latency time.Duration) {
	currentAvg := m.AverageLatency.Load().(time.Duration)
	totalOps := m.SuccessCount.Load()

	// Calculate new average
	newAvg := time.Duration(int64(float64(currentAvg.Nanoseconds())*float64(totalOps-1) +
		float64(latency.Nanoseconds())/float64(totalOps)))

	m.AverageLatency.Store(newAvg)
}

// GetMetrics returns the current metrics
func (m *Metrics) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"timeout_count":    m.TimeoutCount.Load(),
		"success_count":    m.SuccessCount.Load(),
		"total_operations": m.TotalOperations.Load(),
		"timeout_rate":     m.GetTimeoutRate(),
		"average_latency":  m.AverageLatency.Load().(time.Duration).String(),
		"last_updated":     m.LastUpdated.Load().(time.Time),
	}
}

// MarshalJSON implements json.Marshaler
func (m *Metrics) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.GetMetrics())
}
