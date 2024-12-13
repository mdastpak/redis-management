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
	t.Parallel()

	scales := []int{1, 10, 100, 1000}

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%dx", scale), func(t *testing.T) {
			// Create longer context for larger scales
			timeout := time.Duration(scale) * time.Second
			if timeout < 5*time.Second {
				timeout = 5 * time.Second
			}

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			numOperations := 10 * scale

			newCfg := *rs.cfg
			newCfg.Redis.HashKeys = false
			newCfg.Redis.KeyPrefix = "test_bulk_os:"

			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = numOperations
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			assert.NoError(t, err)

			results := make(chan error, numOperations)
			done := make(chan struct{})

			start := time.Now()

			go func() {
				var wg sync.WaitGroup

				// Use buffered semaphore to control concurrency
				semaphore := make(chan struct{}, 100)

				for i := 0; i < numOperations; i++ {
					wg.Add(1)
					semaphore <- struct{}{}

					go func(i int) {
						defer wg.Done()
						defer func() { <-semaphore }()

						key := fmt.Sprintf("key_%d", i)
						value := fmt.Sprintf("value_%d", i)

						err := rs.AddBulkOperation(ctx, "SET", key, value, (rs.cfg.Redis.TTL * time.Duration(scale)))
						if err != nil {
							select {
							case results <- fmt.Errorf("operation %d failed: %v", i, err):
							default:
							}
						}
					}(i)

					// Add small delay between batches to prevent overwhelming
					if i > 0 && i%rs.cfg.Bulk.BatchSize == 0 {
						time.Sleep(10 * time.Millisecond)
					}

				}

				wg.Wait()
				close(results)
				close(done)
			}()

			// Wait for completion or timeout
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					t.Logf("Operations timed out after %v", time.Since(start))
				}
				t.Fatal("Context cancelled or timed out")
			case <-done:
				// Process any errors
				for err := range results {
					t.Error(err)
				}
			}

			elapsed := time.Since(start)
			opsPerSec := float64(numOperations) / elapsed.Seconds()

			// Wait for bulk operations to complete
			time.Sleep(time.Duration(scale) * 100 * time.Millisecond)

			// Verify a sample of results
			sampleSize := min(numOperations, 100)
			for i := 0; i < sampleSize; i++ {
				index := i * (numOperations / sampleSize)
				key := fmt.Sprintf("key_%d", index)
				expectedValue := fmt.Sprintf("value_%d", index)

				value, err := rs.Get(ctx, key)
				if err != nil {
					t.Errorf("Failed to get key %s: %v", key, err)
					continue
				}
				if value != expectedValue {
					t.Errorf("Value mismatch for key %s: expected %s, got %s",
						key, expectedValue, value)
				}

				// Verify TTL is set
				ttl, err := rs.GetTTL(ctx, key)
				if err != nil {
					t.Errorf("Failed to get TTL for key %s: %v", key, err)
					continue
				}
				require.InDelta(t, (rs.cfg.Redis.TTL * time.Duration(scale)).Seconds(), ttl.Seconds(), 30.0*float64(scale))
			}

			t.Logf("Scale %dx Statistics:", scale)
			t.Logf("- Total Operations: %d", numOperations)
			t.Logf("- Total Time: %v", elapsed)
			t.Logf("- Operations/sec: %.2f", opsPerSec)
			t.Logf("- Avg Time/Operation: %.3f ms",
				float64(elapsed.Milliseconds())/float64(numOperations))

			if stats := rs.GetPoolStats(); stats != nil {
				t.Logf("- Pool Stats:")
				t.Logf("  * Total Connections: %d", stats.TotalConns)
				t.Logf("  * Idle Connections: %d", stats.IdleConns)
			}
		})
	}
}
