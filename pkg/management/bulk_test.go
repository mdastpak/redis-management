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

const (
	bulkOperationWaitTime = 2 * time.Second
)

func TestBulkOperations(t *testing.T) {
	t.Parallel()

	t.Run("Bulk Operations with Default TTL", func(t *testing.T) {
		timeout := time.Duration(1) * time.Second
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

		numOperations := 10

		newCfg := *rs.cfg
		newCfg.Redis.HashKeys = false
		newCfg.Redis.KeyPrefix = "bulk_operations_with_default_ttl:"

		newCfg.Bulk.Status = true
		newCfg.Bulk.BatchSize = numOperations
		newCfg.Bulk.FlushInterval = 1
		newCfg.Bulk.MaxRetries = 3
		newCfg.Bulk.FlushInterval = 1
		newCfg.Bulk.ConcurrentFlush = true

		err = rs.ReloadConfig(&newCfg)
		assert.NoError(t, err)

		var wg sync.WaitGroup

		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("key_%d", i)
				value := fmt.Sprintf("value_%d", i)

				err := rs.AddBulkOperation(ctx, "SET", key, value, rs.cfg.Redis.TTL)
				require.NoError(t, err)
			}(i)
		}

		wg.Wait()
		time.Sleep(bulkOperationWaitTime) // Allow bulk operations to complete

		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("key_%d", i)
			expectedValue := fmt.Sprintf("value_%d", i)

			assertKeyValue(t, rs, key, expectedValue)

			// Verify TTL is set
			ttl, err := rs.GetTTL(ctx, key)
			if err != nil {
				t.Errorf("Failed to get TTL for key %s: %v", key, err)
				continue
			}
			require.InDelta(t, rs.cfg.Redis.TTL.Seconds(), ttl.Seconds(), 10)
		}
	})

	t.Run("Bulk Operations with Mixed TTLs", func(t *testing.T) {
		timeout := time.Duration(10) * time.Second
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

		numOperations := 10

		newCfg := *rs.cfg
		newCfg.Redis.HashKeys = false
		newCfg.Redis.KeyPrefix = "bulk_operations_with_mixed_ttls:"

		newCfg.Bulk.Status = true
		newCfg.Bulk.BatchSize = numOperations
		newCfg.Bulk.FlushInterval = 1
		newCfg.Bulk.MaxRetries = 3
		newCfg.Bulk.FlushInterval = 1
		newCfg.Bulk.ConcurrentFlush = true

		err = rs.ReloadConfig(&newCfg)
		assert.NoError(t, err)

		var wg sync.WaitGroup

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

		wg.Wait()
		time.Sleep(bulkOperationWaitTime)

		// Verify each operation
		for _, op := range operations {
			assertKeyValue(t, rs, op.key, op.value)
			expectedTTL := op.ttl
			t.Log("Key, Expected TTL:", op.key, expectedTTL)
			if expectedTTL == 0 {
				expectedTTL = time.Duration(-1)
			}

			// Verify TTL is set
			ttl, err := rs.GetTTL(ctx, op.key)
			t.Log("Actual TTL:", ttl)
			if err != nil {
				t.Errorf("Failed to get TTL for key %s: %v", op.key, err)
				continue
			}
			require.InDelta(t, op.ttl.Seconds(), ttl.Seconds(), 10)

		}
	})
}
