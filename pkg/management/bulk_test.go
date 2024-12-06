// pkg/management/bulk_test.go
package management

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	bulkOperationWaitTime = 2 * time.Second
)

func TestBulkOperations(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

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

				err := service.AddBulkOperation(ctx, "SET", key, value, cfg.Redis.TTL)
				require.NoError(t, err)
			}(i)
		}

		wg.Wait()
		time.Sleep(bulkOperationWaitTime) // Allow bulk operations to complete

		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("bulk_key_%d", i)
			expectedValue := fmt.Sprintf("value_%d", i)

			assertKeyValue(t, service, key, expectedValue)
			assertTTL(t, service, key, cfg.Redis.TTL)
		}
	})

	t.Run("Bulk Operations with Mixed TTLs", func(t *testing.T) {
		ctx := context.Background()

		// Test mixed TTL values
		operations := []struct {
			key   string
			value string
			ttl   time.Duration
		}{
			{"mixed_ttl_1", "value1", 0},             // No TTL
			{"mixed_ttl_2", "value2", cfg.Redis.TTL}, // Default TTL
			{"mixed_ttl_3", "value3", time.Hour},     // Custom TTL
		}

		for _, op := range operations {
			err := service.AddBulkOperation(ctx, "SET", op.key, op.value, op.ttl)
			require.NoError(t, err)
		}

		time.Sleep(bulkOperationWaitTime)

		// Verify each operation
		for _, op := range operations {
			assertKeyValue(t, service, op.key, op.value)
			expectedTTL := op.ttl
			if expectedTTL == 0 {
				expectedTTL = time.Duration(-1)
			}
			assertTTL(t, service, op.key, expectedTTL)
		}
	})
}
