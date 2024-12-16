package management

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceUsage(t *testing.T) {
	scales := []int{1, 10, 100, 1000}

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%dx", scale), func(t *testing.T) {
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

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			startAlloc := m.Alloc
			startSys := m.Sys

			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = scale * 10 // Scale batch size appropriately
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			// Additional scale-specific configurations
			if scale >= 100 {
				newCfg.Bulk.BatchSize = scale * 20 // Larger batches for bigger scales
				newCfg.Pool.Size = scale * 2       // Increase pool size if needed
			}

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			// Sleep briefly to allow bulk processor to initialize
			time.Sleep(100 * time.Millisecond)

			// Verify bulk processor is initialized
			require.NotNil(t, rs.bulkProcessor, "Bulk processor should be initialized")

			numOperations := 10 * scale
			var wg sync.WaitGroup
			start := time.Now()

			// Use buffered semaphore to control concurrency
			semaphore := make(chan struct{}, 100)

			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					// Acquire semaphore
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					key := fmt.Sprintf("key_%d", i)
					value := fmt.Sprintf("value_%d", i)

					err := rs.AddBulkOperation(ctx, "SET", key, value, time.Hour)
					require.NoError(t, err)
				}(i)
			}

			wg.Wait()

			runtime.ReadMemStats(&m)
			endAlloc := m.Alloc
			endSys := m.Sys
			elapsed := time.Since(start)

			t.Logf("Scale %dx Statistics:", scale)
			t.Logf("- Operations: %d", numOperations)
			t.Logf("- Time: %v", elapsed)
			t.Logf("- Ops/sec: %.2f", float64(numOperations)/elapsed.Seconds())
			t.Logf("- Memory Allocated: %d bytes", endAlloc-startAlloc)
			t.Logf("- System Memory: %d bytes", endSys-startSys)
			t.Logf("- Memory per Operation: %.2f bytes", float64(endAlloc-startAlloc)/float64(numOperations))

			// Allow time for bulk operations to complete
			time.Sleep(time.Duration(newCfg.Bulk.FlushInterval+1) * time.Second)

			// Verify operations were successful
			sampleSize := min(numOperations, 10)
			for i := 0; i < sampleSize; i++ {
				key := fmt.Sprintf("key_%d", i)
				expectedValue := fmt.Sprintf("value_%d", i)
				value, err := rs.Get(ctx, key)
				require.NoError(t, err)
				assert.Equal(t, expectedValue, value)
			}
		})
	}
}
