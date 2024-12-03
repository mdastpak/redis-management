package management

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkOperationsScalability(t *testing.T) {
	// Define test scales: 1x, 10x, 100x, 1000x
	scales := []int{1, 10, 100, 1000}

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%dx", scale), func(t *testing.T) {
			// Calculate number of operations based on scale
			numOperations := 10 * scale
			mr, cfg := setupTestRedis(t)
			defer mr.Close()

			// Configure batch size according to scale
			cfg.Bulk.BatchSize = 10 * scale
			service, err := NewRedisService(cfg)
			require.NoError(t, err)
			defer service.Close()

			// Track execution time and prepare error handling
			start := time.Now()
			var wg sync.WaitGroup
			errChan := make(chan error, numOperations)

			// Execute bulk operations concurrently
			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := fmt.Sprintf("scale_test:key:%d", i)
					value := fmt.Sprintf("value:%d", i)
					ctx := context.Background()

					// Attempt bulk operation
					err := service.AddBulkOperation(ctx, "SET", key, value, time.Hour)
					if err != nil {
						errChan <- fmt.Errorf("operation %d failed: %v", i, err)
					}
				}(i)
			}

			// Wait for all operations to complete
			wg.Wait()
			close(errChan)

			// Check for any errors during execution
			for err := range errChan {
				t.Error(err)
			}

			// Calculate and log performance metrics
			elapsed := time.Since(start)
			opsPerSec := float64(numOperations) / elapsed.Seconds()

			t.Logf("Scale %dx: Processed %d operations in %v (%.2f ops/sec)",
				scale, numOperations, elapsed, opsPerSec)

			// Verify all operations were successful
			for i := 0; i < numOperations; i++ {
				key := fmt.Sprintf("test:scale_test:key:%d", i)
				expectedValue := fmt.Sprintf("value:%d", i)

				// Verify each key-value pair
				value, err := mr.Get(key)
				require.NoError(t, err, "Failed to get key %s", key)
				assert.Equal(t, expectedValue, value,
					"Value mismatch for key %s", key)
			}

			// Log additional metrics for analysis
			t.Logf("Average time per operation: %.3f ms",
				float64(elapsed.Milliseconds())/float64(numOperations))

			stats := service.getPoolStats()
			if stats != nil {
				t.Logf("Pool stats - TotalConns: %d, IdleConns: %d",
					stats.TotalConns, stats.IdleConns)
			}
		})
	}
}
