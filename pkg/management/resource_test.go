package management

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResourceUsage(t *testing.T) {
	scales := []int{1, 10, 100, 1000}

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%dx", scale), func(t *testing.T) {
			numOperations := 10 * scale

			rs, err := setupTestRedis()
			require.NoError(t, err)
			defer rs.Close()

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			startAlloc := m.Alloc
			startSys := m.Sys

			rs.cfg.Bulk.BatchSize = 10 * scale

			// service, err := NewRedisService(rs.cfg)
			// require.NoError(t, err)
			// defer service.Close()

			// اجرای عملیات‌ها
			var wg sync.WaitGroup
			ctx := context.Background()

			start := time.Now()

			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := fmt.Sprintf("resource_test:key:%d", i)
					value := fmt.Sprintf("value:%d", i)

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
