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

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			startAlloc := m.Alloc
			startSys := m.Sys

			newCfg := *rs.cfg

			newCfg.Redis.HashKeys = false
			newCfg.Redis.KeyPrefix = "test_resource_usage:"

			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = numOperations
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 1
			newCfg.Bulk.FlushInterval = 1

			err = rs.ReloadConfig(&newCfg)
			assert.NoError(t, err)

			var wg sync.WaitGroup

			start := time.Now()

			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
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
		})
	}
}
