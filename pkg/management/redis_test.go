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

	ctx := context.Background()

	t.Run("Set and Get", func(t *testing.T) {
		err := service.Set(ctx, "test_key", "test_value", time.Hour)
		assert.NoError(t, err)

		value, err := service.Get(ctx, "test_key")
		assert.NoError(t, err)
		assert.Equal(t, "test_value", value)
	})

	t.Run("Get Non-Existent Key", func(t *testing.T) {
		value, err := service.Get(ctx, "non_existent")
		assert.Error(t, err)
		assert.Empty(t, value)
	})
}
