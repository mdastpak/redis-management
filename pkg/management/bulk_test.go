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
	t.Parallel()

	t.Run("Bulk Operations with Default TTL", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		numOperations := 10
		var wg sync.WaitGroup

		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("bulk_key_%d", i)
				value := fmt.Sprintf("value_%d", i)

				err := rs.AddBulkOperation(ctx, "SET", key, value, rs.cfg.Redis.TTL)
				require.NoError(t, err)
			}(i)
		}

		wg.Wait()
		time.Sleep(bulkOperationWaitTime) // Allow bulk operations to complete

		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("bulk_key_%d", i)
			expectedValue := fmt.Sprintf("value_%d", i)

			assertKeyValue(t, rs, key, expectedValue)
			assertTTL(t, rs, key, rs.cfg.Redis.TTL)
		}
	})

	t.Run("Bulk Operations with Mixed TTLs", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		// Test mixed TTL values
		operations := []struct {
			key   string
			value string
			ttl   time.Duration
		}{
			{"mixed_ttl_1", "value1", 0},                // No TTL
			{"mixed_ttl_2", "value2", rs.cfg.Redis.TTL}, // Default TTL
			{"mixed_ttl_3", "value3", time.Hour},        // Custom TTL
		}

		for _, op := range operations {
			err := rs.AddBulkOperation(ctx, "SET", op.key, op.value, op.ttl)
			require.NoError(t, err)
		}

		time.Sleep(bulkOperationWaitTime)

		// Verify each operation
		for _, op := range operations {
			assertKeyValue(t, rs, op.key, op.value)
			expectedTTL := op.ttl
			if expectedTTL == 0 {
				expectedTTL = time.Duration(-1)
			}
			assertTTL(t, rs, op.key, expectedTTL)
		}
	})
}
