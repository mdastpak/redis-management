package management

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkOperationsMonitoring(t *testing.T) {
	t.Parallel()

	// Setup test Redis service with monitoring
	rs, ctx, cancel := setupTestRedisWithConfig(t)
	defer cancel()

	// Test parameters
	itemCount := 1000
	monitoringDuration := 5 * time.Second
	samplingInterval := 100 * time.Millisecond

	// Create test data
	items := make(map[string]interface{}, itemCount)
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("monitoring_test_key_%d", i)
		items[key] = fmt.Sprintf("value_%d", i)
	}

	// Start monitoring
	var metrics []*BulkMetrics
	var metricsMutex sync.Mutex
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(samplingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				currentMetrics := rs.GetBulkMetrics()
				metricsMutex.Lock()
				metrics = append(metrics, &currentMetrics)
				metricsMutex.Unlock()
			}
		}
	}()

	// Execute bulk operations
	startTime := time.Now()
	err := rs.BulkSet(ctx, items, time.Hour)
	require.NoError(t, err)

	// Continue monitoring for the specified duration
	time.Sleep(monitoringDuration)
	close(done)

	// Analyze metrics
	metricsMutex.Lock()
	defer metricsMutex.Unlock()

	require.NotEmpty(t, metrics, "Should have collected metrics")

	// Calculate statistical measures
	var totalOps, totalErrors int64
	var maxLatency time.Duration

	for _, m := range metrics {
		totalOps += m.OperationsCount.Load()
		totalErrors += m.ErrorCount.Load()
		if latency, ok := m.AverageLatency.Load().(time.Duration); ok && latency > maxLatency {
			maxLatency = latency
		}
	}

	avgOps := float64(totalOps) / float64(len(metrics))
	errorRate := float64(totalErrors) / float64(totalOps)

	// Log monitoring results
	t.Logf("Monitoring Results:")
	t.Logf("- Total Operations: %d", totalOps)
	t.Logf("- Average Operations per Sample: %.2f", avgOps)
	t.Logf("- Error Rate: %.2f%%", errorRate*100)
	t.Logf("- Maximum Latency: %v", maxLatency)
	t.Logf("- Monitoring Duration: %v", time.Since(startTime))
	t.Logf("- Number of Samples: %d", len(metrics))

	// Assertions
	assert.Less(t, errorRate, 0.1, "Error rate should be less than 10%")
	assert.Greater(t, avgOps, float64(0), "Should have processed operations")
	assert.Greater(t, len(metrics), 0, "Should have collected multiple metrics samples")
}

// Helper function to split items into batches
func splitIntoBatches(items map[string]interface{}, batchSize int) []map[string]interface{} {
	var batches []map[string]interface{}
	batch := make(map[string]interface{})
	count := 0

	for k, v := range items {
		batch[k] = v
		count++

		if count == batchSize {
			batches = append(batches, batch)
			batch = make(map[string]interface{})
			count = 0
		}
	}

	if len(batch) > 0 {
		batches = append(batches, batch)
	}

	return batches
}
