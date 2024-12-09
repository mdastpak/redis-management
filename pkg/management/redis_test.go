// pkg/management/redis_test.go
package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mdastpak/redis-management/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to be used across all test files
func setupTestRedis() (*RedisService, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

	rs, err := NewRedisService(cfg)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func TestRedisService_BasicOperations(t *testing.T) {
	t.Parallel()

	t.Run("Set with No Expiration", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		key := "permanent_key"
		value := "test_value"

		err = rs.Set(ctx, key, value, 0)
		require.NoError(t, err)

		assertKeyValue(t, rs, key, value)
		assertTTL(t, rs, key, time.Duration(-1))
	})

	t.Run("Set with Default TTL", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		key := "default_ttl_key"
		value := "test_value"

		err = rs.SetWithDefaultTTL(ctx, key, value)
		require.NoError(t, err)

		assertKeyValue(t, rs, key, value)
		assertTTL(t, rs, key, rs.cfg.Redis.TTL)
	})

	t.Run("SetBatch with Different TTLs", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		// No TTL batch
		noTTLItems := map[string]interface{}{
			"batch_no_ttl_1": "value1",
			"batch_no_ttl_2": "value2",
		}
		err = rs.SetBatch(ctx, noTTLItems, 0)
		require.NoError(t, err)

		// Default TTL batch
		defaultTTLItems := map[string]interface{}{
			"batch_default_1": "value1",
			"batch_default_2": "value2",
		}
		err = rs.SetBatchWithDefaultTTL(ctx, defaultTTLItems)
		require.NoError(t, err)

		// Verify no TTL batch
		for key, expectedValue := range noTTLItems {
			assertKeyValue(t, rs, key, expectedValue)
			assertTTL(t, rs, key, time.Duration(-1))
		}

		// Verify default TTL batch
		for key, expectedValue := range defaultTTLItems {
			assertKeyValue(t, rs, key, expectedValue)
			assertTTL(t, rs, key, rs.cfg.Redis.TTL)
		}
	})

	t.Run("SetBatch with Zero TTL", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		// Explicit zero TTL
		zeroTTLItems := map[string]interface{}{
			"zero_ttl_1": "value1",
			"zero_ttl_2": "value2",
		}
		err = rs.SetBatch(ctx, zeroTTLItems, 0)
		require.NoError(t, err)

		// No TTL specified (should be same as zero)
		noTTLItems := map[string]interface{}{
			"no_ttl_1": "value1",
			"no_ttl_2": "value2",
		}
		err = rs.SetBatch(ctx, noTTLItems, 0)
		require.NoError(t, err)

		// Verify both behave the same
		for key, expectedValue := range zeroTTLItems {
			assertKeyValue(t, rs, key, expectedValue)
			assertTTL(t, rs, key, time.Duration(-1))
		}

		for key, expectedValue := range noTTLItems {
			assertKeyValue(t, rs, key, expectedValue)
			assertTTL(t, rs, key, time.Duration(-1))
		}
	})
}

func TestTTLOperations(t *testing.T) {
	t.Parallel()

	t.Run("GetTTL and SetTTL Single Key", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		key := "ttl_test_key"
		value := "test_value"
		expectedTTL := 1 * time.Hour

		// Set key with value
		err = rs.Set(ctx, key, value, expectedTTL)
		require.NoError(t, err)

		// Get TTL
		finalKey := rs.keyMgr.GetKey(key)
		ttl, err := rs.GetTTL(ctx, key)
		require.NoError(t, err)
		t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
		assert.Equal(t, expectedTTL, ttl)

		// Update TTL
		newTTL := 30 * time.Minute
		err = rs.SetTTL(ctx, key, newTTL)
		require.NoError(t, err)

		// Verify updated TTL
		ttl, err = rs.GetTTL(ctx, key)
		require.NoError(t, err)
		t.Logf("Updated TTL for key %s (final: %s): %v", key, finalKey, ttl)
		assert.Equal(t, newTTL, ttl)

		// Verify value remains unchanged
		value, err = rs.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "test_value", value)
	})

	t.Run("GetBatchTTL and SetBatchTTL", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		keys := []string{
			"batch_ttl_1",
			"batch_ttl_2",
			"batch_ttl_3",
		}
		initialTTL := 2 * time.Hour

		// Set initial keys with values and TTL
		for _, key := range keys {
			err := rs.Set(ctx, key, fmt.Sprintf("value_%s", key), initialTTL)
			require.NoError(t, err)
		}

		// Get batch TTL
		ttls, err := rs.GetBatchTTL(ctx, keys)
		require.NoError(t, err)

		for key, ttl := range ttls {
			finalKey := rs.keyMgr.GetKey(key)
			t.Logf("Initial TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, initialTTL, ttl)
		}

		// Update batch TTL
		newTTL := 45 * time.Minute
		err = rs.SetBatchTTL(ctx, keys, newTTL)
		require.NoError(t, err)

		// Verify updated TTLs
		ttls, err = rs.GetBatchTTL(ctx, keys)
		require.NoError(t, err)

		for key, ttl := range ttls {
			finalKey := rs.keyMgr.GetKey(key)
			t.Logf("Updated TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, newTTL, ttl)

			// Verify values remain unchanged
			value, err := rs.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("value_%s", key), value)
		}
	})

	t.Run("GetTTL Non-Existent Key", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		key := "non_existent_key"
		ttl, err := rs.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(-2), ttl, "Non-existent key should return -2")
	})

	t.Run("SetTTL Non-Existent Key", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		key := "non_existent_key"
		err = rs.SetTTL(ctx, key, time.Hour)
		require.Error(t, err, "Setting TTL on non-existent key should fail")
	})

	t.Run("GetBatchTTL Mixed Keys", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		// Prepare: one existing key, one non-existent
		existingKey := "existing_key"
		err = rs.Set(ctx, existingKey, "value", time.Hour)
		require.NoError(t, err)

		keys := []string{
			existingKey,
			"non_existent_key",
		}

		ttls, err := rs.GetBatchTTL(ctx, keys)
		require.NoError(t, err)

		finalExistingKey := rs.keyMgr.GetKey(existingKey)
		t.Logf("TTL for existing key %s (final: %s): %v", existingKey, finalExistingKey, ttls[existingKey])
		assert.Equal(t, time.Hour, ttls[existingKey])

		finalNonExistentKey := rs.keyMgr.GetKey("non_existent_key")
		t.Logf("TTL for non-existent key (final: %s): %v", finalNonExistentKey, ttls["non_existent_key"])
		assert.Equal(t, time.Duration(-2), ttls["non_existent_key"])
	})

	t.Run("Zero TTL Operations", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		key := "zero_ttl_key"

		// Set key with no TTL
		err = rs.Set(ctx, key, "value", 0)
		require.NoError(t, err)

		// Verify no TTL
		ttl, err := rs.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(-1), ttl, "Key with no TTL should return -1")

		// Set TTL
		err = rs.SetTTL(ctx, key, time.Hour)
		require.NoError(t, err)

		// Remove TTL by setting it to 0
		err = rs.SetTTL(ctx, key, 0)
		require.NoError(t, err)

		// Verify TTL was removed
		ttl, err = rs.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(-1), ttl, "Key should have no TTL after setting it to 0")
	})
}
