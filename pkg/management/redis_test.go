// pkg/management/redis_test.go
package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mdastpak/redis-management/config"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function for checking TTL
func assertTTL(t *testing.T, service *RedisService, key string, expectedTTL time.Duration) {
	ttl, err := service.GetTTL(context.Background(), key)
	require.NoError(t, err)
	t.Logf("TTL for key %s: %v", key, ttl)
	assert.Equal(t, expectedTTL, ttl)
}

// Helper function for checking key value
func assertKeyValue(t *testing.T, service *RedisService, key string, expectedValue interface{}) {
	value, err := service.Get(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, expectedValue, value)
}

// Helper function to be used across all test files
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *config.Config) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	cfg := &config.Config{
		Redis: config.RedisConfig{
			Host:                mr.Host(),
			Port:                mr.Port(),
			Password:            "",
			DB:                  "0",
			KeyPrefix:           "test:",
			Timeout:             5,
			TTL:                 60 * time.Second,
			HashKeys:            false,
			HealthCheckInterval: 0, // Disable health checks for testing
			RetryAttempts:       2,
			RetryDelay:          100 * time.Millisecond,
			MaxRetryBackoff:     1000 * time.Millisecond,
		},
		Pool: config.PoolConfig{
			Status:      true,
			Size:        5,
			MinIdle:     1,
			MaxIdleTime: 60,
			WaitTimeout: 5,
		},
		Bulk: config.BulkConfig{
			Status:          true,
			BatchSize:       10,
			FlushInterval:   1,
			MaxRetries:      2,
			ConcurrentFlush: true,
		},
	}

	return mr, cfg
}

func TestRedisService_BasicOperations(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	t.Run("Set with No Expiration", func(t *testing.T) {
		ctx := context.Background()
		key := "permanent_key"
		value := "test_value"

		err := service.Set(ctx, key, value, 0)
		require.NoError(t, err)

		assertKeyValue(t, service, key, value)
		assertTTL(t, service, key, time.Duration(-1))
	})

	t.Run("Set with Default TTL", func(t *testing.T) {
		ctx := context.Background()
		key := "default_ttl_key"
		value := "test_value"

		err := service.SetWithDefaultTTL(ctx, key, value)
		require.NoError(t, err)

		assertKeyValue(t, service, key, value)
		assertTTL(t, service, key, cfg.Redis.TTL)
	})

	t.Run("SetBatch with Different TTLs", func(t *testing.T) {
		ctx := context.Background()

		// No TTL batch
		noTTLItems := map[string]interface{}{
			"batch_no_ttl_1": "value1",
			"batch_no_ttl_2": "value2",
		}
		err := service.SetBatch(ctx, noTTLItems, 0)
		require.NoError(t, err)

		// Default TTL batch
		defaultTTLItems := map[string]interface{}{
			"batch_default_1": "value1",
			"batch_default_2": "value2",
		}
		err = service.SetBatchWithDefaultTTL(ctx, defaultTTLItems)
		require.NoError(t, err)

		// Verify no TTL batch
		for key, expectedValue := range noTTLItems {
			assertKeyValue(t, service, key, expectedValue)
			assertTTL(t, service, key, time.Duration(-1))
		}

		// Verify default TTL batch
		for key, expectedValue := range defaultTTLItems {
			assertKeyValue(t, service, key, expectedValue)
			assertTTL(t, service, key, cfg.Redis.TTL)
		}
	})

	t.Run("SetBatch with Zero TTL", func(t *testing.T) {
		ctx := context.Background()

		// Explicit zero TTL
		zeroTTLItems := map[string]interface{}{
			"zero_ttl_1": "value1",
			"zero_ttl_2": "value2",
		}
		err := service.SetBatch(ctx, zeroTTLItems, 0)
		require.NoError(t, err)

		// No TTL specified (should be same as zero)
		noTTLItems := map[string]interface{}{
			"no_ttl_1": "value1",
			"no_ttl_2": "value2",
		}
		err = service.SetBatch(ctx, noTTLItems, 0)
		require.NoError(t, err)

		// Verify both behave the same
		for key, expectedValue := range zeroTTLItems {
			assertKeyValue(t, service, key, expectedValue)
			assertTTL(t, service, key, time.Duration(-1))
		}

		for key, expectedValue := range noTTLItems {
			assertKeyValue(t, service, key, expectedValue)
			assertTTL(t, service, key, time.Duration(-1))
		}
	})
}

func TestTTLOperations(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	ctx := context.Background()

	t.Run("GetTTL and SetTTL Single Key", func(t *testing.T) {
		key := "ttl_test_key"
		value := "test_value"
		expectedTTL := 1 * time.Hour

		// Set key with value
		err := service.Set(ctx, key, value, expectedTTL)
		require.NoError(t, err)

		// Get TTL
		finalKey := service.keyMgr.GetKey(key)
		ttl, err := service.GetTTL(ctx, key)
		require.NoError(t, err)
		t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
		assert.Equal(t, expectedTTL, ttl)

		// Update TTL
		newTTL := 30 * time.Minute
		err = service.SetTTL(ctx, key, newTTL)
		require.NoError(t, err)

		// Verify updated TTL
		ttl, err = service.GetTTL(ctx, key)
		require.NoError(t, err)
		t.Logf("Updated TTL for key %s (final: %s): %v", key, finalKey, ttl)
		assert.Equal(t, newTTL, ttl)

		// Verify value remains unchanged
		value, err = service.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "test_value", value)
	})

	t.Run("GetBatchTTL and SetBatchTTL", func(t *testing.T) {
		keys := []string{
			"batch_ttl_1",
			"batch_ttl_2",
			"batch_ttl_3",
		}
		initialTTL := 2 * time.Hour

		// Set initial keys with values and TTL
		for _, key := range keys {
			err := service.Set(ctx, key, fmt.Sprintf("value_%s", key), initialTTL)
			require.NoError(t, err)
		}

		// Get batch TTL
		ttls, err := service.GetBatchTTL(ctx, keys)
		require.NoError(t, err)

		for key, ttl := range ttls {
			finalKey := service.keyMgr.GetKey(key)
			t.Logf("Initial TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, initialTTL, ttl)
		}

		// Update batch TTL
		newTTL := 45 * time.Minute
		err = service.SetBatchTTL(ctx, keys, newTTL)
		require.NoError(t, err)

		// Verify updated TTLs
		ttls, err = service.GetBatchTTL(ctx, keys)
		require.NoError(t, err)

		for key, ttl := range ttls {
			finalKey := service.keyMgr.GetKey(key)
			t.Logf("Updated TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, newTTL, ttl)

			// Verify values remain unchanged
			value, err := service.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("value_%s", key), value)
		}
	})

	t.Run("GetTTL Non-Existent Key", func(t *testing.T) {
		key := "non_existent_key"
		ttl, err := service.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(-2), ttl, "Non-existent key should return -2")
	})

	t.Run("SetTTL Non-Existent Key", func(t *testing.T) {
		key := "non_existent_key"
		err := service.SetTTL(ctx, key, time.Hour)
		require.Error(t, err, "Setting TTL on non-existent key should fail")
	})

	t.Run("GetBatchTTL Mixed Keys", func(t *testing.T) {
		// Prepare: one existing key, one non-existent
		existingKey := "existing_key"
		err := service.Set(ctx, existingKey, "value", time.Hour)
		require.NoError(t, err)

		keys := []string{
			existingKey,
			"non_existent_key",
		}

		ttls, err := service.GetBatchTTL(ctx, keys)
		require.NoError(t, err)

		finalExistingKey := service.keyMgr.GetKey(existingKey)
		t.Logf("TTL for existing key %s (final: %s): %v", existingKey, finalExistingKey, ttls[existingKey])
		assert.Equal(t, time.Hour, ttls[existingKey])

		finalNonExistentKey := service.keyMgr.GetKey("non_existent_key")
		t.Logf("TTL for non-existent key (final: %s): %v", finalNonExistentKey, ttls["non_existent_key"])
		assert.Equal(t, time.Duration(-2), ttls["non_existent_key"])
	})

	t.Run("Zero TTL Operations", func(t *testing.T) {
		key := "zero_ttl_key"

		// Set key with no TTL
		err := service.Set(ctx, key, "value", 0)
		require.NoError(t, err)

		// Verify no TTL
		ttl, err := service.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(-1), ttl, "Key with no TTL should return -1")

		// Set TTL
		err = service.SetTTL(ctx, key, time.Hour)
		require.NoError(t, err)

		// Remove TTL by setting it to 0
		err = service.SetTTL(ctx, key, 0)
		require.NoError(t, err)

		// Verify TTL was removed
		ttl, err = service.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(-1), ttl, "Key should have no TTL after setting it to 0")
	})
}
