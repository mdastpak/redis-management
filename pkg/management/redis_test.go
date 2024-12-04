// pkg/management/redis_test.go
package management

import (
	"context"
	"testing"
	"time"

	"github.com/mdastpak/redis-management/config"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

		err := service.Set(ctx, key, "test_value", 0)
		require.NoError(t, err)

		finalKey := service.keyMgr.GetKey(key)
		value, err := service.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "test_value", value)

		ttl := mr.TTL(finalKey)
		t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
		assert.Equal(t, time.Duration(0), ttl)
	})

	t.Run("Set with Default TTL", func(t *testing.T) {
		ctx := context.Background()
		key := "default_ttl_key"

		err := service.SetWithDefaultTTL(ctx, key, "test_value")
		require.NoError(t, err)

		// Get final key with prefix/hash
		finalKey := service.keyMgr.GetKey(key)

		value, err := service.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "test_value", value)

		ttl := mr.TTL(finalKey)
		t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
		assert.Equal(t, time.Duration(cfg.Redis.TTL), ttl)
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
			finalKey := service.keyMgr.GetKey(key)

			value, err := service.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)

			ttl := mr.TTL(finalKey)
			t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, time.Duration(0), ttl)
		}

		// Verify default TTL batch
		for key, expectedValue := range defaultTTLItems {
			finalKey := service.keyMgr.GetKey(key)

			value, err := service.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)

			ttl := mr.TTL(finalKey)
			t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, time.Duration(cfg.Redis.TTL), ttl)
		}
	})
}
