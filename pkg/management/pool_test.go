// pkg/management/pool_test.go
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

func TestConnectionPool(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	t.Run("Pool Statistics", func(t *testing.T) {
		// Allow pool to initialize
		time.Sleep(time.Second)

		stats := service.getPoolStats()
		require.NotNil(t, stats, "Pool stats should not be nil")

		// Convert config values to uint32 for comparison
		maxSize := uint32(cfg.Pool.Size)
		minIdle := uint32(cfg.Pool.MinIdle)

		// Check total connections
		assert.True(t, stats.TotalConns <= maxSize,
			"Total connections (%d) should not exceed pool size (%d)",
			stats.TotalConns, maxSize)

		// Check idle connections
		assert.True(t, stats.IdleConns >= minIdle,
			"Idle connections (%d) should be at least min idle (%d)",
			stats.IdleConns, minIdle)
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		ctx := context.Background()
		const numOperations = 10
		var wg sync.WaitGroup
		errs := make([]error, numOperations)

		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("concurrent_key_%d", i)
				errs[i] = service.Set(ctx, key, i, time.Hour)
			}(i)
		}

		wg.Wait()

		// Check for errors
		for i, err := range errs {
			assert.NoError(t, err, "Operation %d failed", i)
		}
	})
}
