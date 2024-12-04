// pkg/management/bulk_test.go
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

func TestBulkOperations(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	cfg.Redis.TTL = 30 * time.Minute
	cfg.Bulk.Status = true
	cfg.Bulk.BatchSize = 5
	cfg.Bulk.FlushInterval = 1

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	t.Run("Bulk Operations with Default TTL", func(t *testing.T) {
		ctx := context.Background()
		numOperations := 10
		var wg sync.WaitGroup

		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("bulk_key_%d", i)
				value := fmt.Sprintf("value_%d", i)

				err := service.AddBulkOperation(ctx, "SET", key, value, 0)
				require.NoError(t, err)
			}(i)
		}

		wg.Wait()
		time.Sleep(2 * time.Second)

		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("bulk_key_%d", i)
			finalKey := service.keyMgr.GetKey(key)

			value, err := mr.Get(finalKey)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("value_%d", i), value)

			ttl := mr.TTL(finalKey)
			t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, time.Duration(0), ttl)
		}
	})
}
