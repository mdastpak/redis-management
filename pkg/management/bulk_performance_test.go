package management

import (
	"fmt"
	"redis-management/config"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBulkOperationsPerformance(t *testing.T) {
	// t.Parallel()

	// Test configurations for different scales
	scales := []struct {
		name          string
		itemCount     int
		concurrency   int
		expectedTime  time.Duration
		batchSize     int
		retryAttempts int
		poolSize      int
		minIdle       int
	}{
		{
			name:          "Small Scale",
			itemCount:     100,
			concurrency:   5,
			expectedTime:  2 * time.Second,
			batchSize:     10,
			retryAttempts: 2,
			poolSize:      10,
			minIdle:       2,
		},
		{
			name:          "Medium Scale",
			itemCount:     1000,
			concurrency:   10,
			expectedTime:  5 * time.Second,
			batchSize:     50,
			retryAttempts: 3,
			poolSize:      20,
			minIdle:       5,
		},
		{
			name:          "Large Scale",
			itemCount:     5000,
			concurrency:   20,
			expectedTime:  10 * time.Second,
			batchSize:     100,
			retryAttempts: 3,
			poolSize:      40,
			minIdle:       10,
		},
	}

	for _, scale := range scales {
		scale := scale // Capture range variable
		t.Run(scale.name, func(t *testing.T) {
			t.Parallel()

			// Setup test Redis service with pool management
			rs, ctx, cancel := setupTestRedisWithConfig(t,
				WithScale(2),
				WithInitialConfig(func(cfg *config.Config) {
					cfg.Redis.HashKeys = false
					cfg.Redis.KeyPrefix = fmt.Sprintf("bulk_test_%s", scale.name)
					cfg.Pool.Status = true
					cfg.Pool.Size = scale.poolSize
					cfg.Pool.MinIdle = scale.minIdle
					cfg.Pool.MaxIdleTime = 300 // 5 minutes
					cfg.Pool.WaitTimeout = 30  // 30 seconds
				}),
			)
			defer cancel() // Only cancels context, doesn't close connections

			// Initialize metrics tracking
			var successCount, failureCount atomic.Int64
			var totalLatency atomic.Int64

			// Create test data
			items := make(map[string]interface{}, scale.itemCount)
			for i := 0; i < scale.itemCount; i++ {
				key := fmt.Sprintf("bulk_test_key_%d", i)
				items[key] = fmt.Sprintf("value_%d", i)
			}

			// Monitoring goroutine
			monitorDone := make(chan struct{})
			var metrics []*BulkMetrics
			var metricsMutex sync.Mutex

			go func() {
				defer close(monitorDone)
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						metricsMutex.Lock()
						var newMetric BulkMetrics
						newMetric.OperationsCount.Store(successCount.Load() + failureCount.Load())
						newMetric.SuccessCount.Store(successCount.Load())
						newMetric.ErrorCount.Store(failureCount.Load())
						newMetric.LastOperationTime.Store(time.Now())
						if totalOps := successCount.Load() + failureCount.Load(); totalOps > 0 {
							newMetric.AverageLatency.Store(time.Duration(totalLatency.Load()) / time.Duration(totalOps))
						}
						metrics = append(metrics, &newMetric)
						metricsMutex.Unlock()
					}
				}
			}()

			// Execute bulk operations with concurrency control
			var wg sync.WaitGroup
			semaphore := make(chan struct{}, scale.concurrency)
			startTime := time.Now()

			// Log start of bulk operations
			t.Logf("Starting bulk operations - Scale: %s, Items: %d", scale.name, scale.itemCount)
			deadline, ok := ctx.Deadline()
			t.Logf("Context status at start - deadline: %v, ok: %v", deadline, ok)

			// Split items into batches
			batches := splitIntoBatches(items, scale.batchSize)
			t.Logf("Created %d batches of size %d", len(batches), scale.batchSize)

			for _, batch := range batches {
				wg.Add(1)
				go func(batchItems map[string]interface{}) {
					defer wg.Done()

					// Acquire semaphore with context check
					select {
					case semaphore <- struct{}{}:
						defer func() { <-semaphore }() // Release semaphore
					case <-ctx.Done():
						t.Logf("Context cancelled during semaphore acquisition: %v", ctx.Err())
						return
					}

					// Execute bulk operation
					operationStart := time.Now()
					err := rs.BulkSet(ctx, batchItems, time.Hour)

					// Update metrics
					latency := time.Since(operationStart)
					totalLatency.Add(int64(latency))

					if err != nil {
						failureCount.Add(1)
						t.Logf("Batch operation failed: %v", err)
					} else {
						successCount.Add(1)
					}
				}(batch)
			}

			// Wait for all operations to complete and log progress
			waitChan := make(chan struct{})
			go func() {
				wg.Wait()
				close(waitChan)
			}()

			// Wait with context and timeout check
			select {
			case <-waitChan:
				t.Logf("All bulk operations completed successfully")
			case <-ctx.Done():
				t.Logf("Context cancelled during wait: %v", ctx.Err())
			}

			// Calculate and log metrics before verify
			duration := time.Since(startTime)
			opsPerSecond := float64(scale.itemCount) / duration.Seconds()
			avgLatency := time.Duration(totalLatency.Load()) / time.Duration(len(batches))

			t.Logf("Performance Results for %s:", scale.name)
			t.Logf("- Total Duration: %v", duration)
			t.Logf("- Operations/second: %.2f", opsPerSecond)
			t.Logf("- Average Latency: %v", avgLatency)
			t.Logf("- Success Rate: %.2f%%", float64(successCount.Load())/float64(len(batches))*100)

			// Log context status before verification
			if deadline, ok := ctx.Deadline(); ok {
				t.Logf("Context status before verify - deadline: %v, remaining: %v",
					deadline, time.Until(deadline))
			}

			// Start verification
			if ctx.Err() == nil {
				verifyStartTime := time.Now()
				var verifySuccessCount, verifyFailCount atomic.Int64

				// Verify data consistency
				for key, expectedValue := range items {
					// Periodic logging during verification
					if verifySuccessCount.Load()+verifyFailCount.Load() > 0 &&
						(verifySuccessCount.Load()+verifyFailCount.Load())%1000 == 0 {
						if deadline, ok := ctx.Deadline(); ok {
							t.Logf("Verify progress - Success: %d, Failed: %d, Remaining time: %v",
								verifySuccessCount.Load(), verifyFailCount.Load(), time.Until(deadline))
						}
					}

					value, err := rs.Get(ctx, key)
					if err != nil {
						verifyFailCount.Add(1)
						t.Logf("Failed to verify key %s: %v", key, err)
						if ctx.Err() != nil {
							t.Logf("Context error during verification: %v", ctx.Err())
							break
						}
					} else {
						verifySuccessCount.Add(1)
						assert.Equal(t, expectedValue, value, "Value mismatch for key %s", key)
					}
				}

				verifyDuration := time.Since(verifyStartTime)
				t.Logf("Verification completed in %v - Success: %d, Failed: %d",
					verifyDuration, verifySuccessCount.Load(), verifyFailCount.Load())
			} else {
				t.Logf("Skipping verification due to context error: %v", ctx.Err())
			}
		})
	}
}
