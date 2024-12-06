package management

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchOperations(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	t.Run("SetBatch with Default TTL", func(t *testing.T) {
		ctx := context.Background()
		items := map[string]interface{}{
			"batch_key1": "value1",
			"batch_key2": "value2",
		}

		err := service.SetBatchWithDefaultTTL(ctx, items)
		require.NoError(t, err)

		for key, expectedValue := range items {
			value, err := service.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)

			ttl, err := service.GetTTL(ctx, key)
			require.NoError(t, err)
			t.Logf("TTL for key %s: %v", key, ttl)
			assert.Equal(t, time.Duration(cfg.Redis.TTL), ttl)
		}
	})
}
