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

// pkg/management/scalability_test.go
func TestBulkOperationsScalability(t *testing.T) {
	scales := []int{1, 10, 100, 1000}

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%dx", scale), func(t *testing.T) {
			numOperations := 10 * scale
			mr, cfg := setupTestRedis(t)
			defer mr.Close()

			cfg.Bulk.BatchSize = 10 * scale
			service, err := NewRedisService(cfg)
			require.NoError(t, err)
			defer service.Close()

			start := time.Now()
			var wg sync.WaitGroup
			errChan := make(chan error, numOperations)

			// اجرای عملیات‌ها
			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := fmt.Sprintf("scale_test:key:%d", i)
					value := fmt.Sprintf("value:%d", i)
					ctx := context.Background()

					err := service.AddBulkOperation(ctx, "SET", key, value, time.Hour)
					if err != nil {
						errChan <- fmt.Errorf("operation %d failed: %v", i, err)
					}
				}(i)
			}

			// انتظار برای تکمیل
			wg.Wait()
			close(errChan)

			// بررسی خطاها
			for err := range errChan {
				t.Error(err)
			}

			elapsed := time.Since(start)
			opsPerSec := float64(numOperations) / elapsed.Seconds()

			t.Logf("Scale %dx: Processed %d operations in %v (%.2f ops/sec)",
				scale, numOperations, elapsed, opsPerSec)

			// verify results
			for i := 0; i < numOperations; i++ {
				key := fmt.Sprintf("test:scale_test:key:%d", i)
				expectedValue := fmt.Sprintf("value:%d", i)
				value, err := mr.Get(key)
				require.NoError(t, err)
				assert.Equal(t, expectedValue, value)
			}
		})
	}
}
