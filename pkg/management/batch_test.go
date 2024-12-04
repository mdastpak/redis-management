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

	cfg.Redis.TTL = 2 * time.Hour
	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	t.Run("SetBatch with Default TTL", func(t *testing.T) {
		ctx := context.Background()
		items := map[string]interface{}{
			"batch_key1": "value1",
			"batch_key2": "value2",
		}

		err := service.SetBatch(ctx, items, 0)
		require.NoError(t, err)

		for key, expectedValue := range items {
			finalKey := service.keyMgr.GetKey(key)

			value, err := mr.Get(finalKey)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)

			ttl := mr.TTL(finalKey)
			t.Logf("TTL for key %s (final: %s): %v", key, finalKey, ttl)
			assert.Equal(t, time.Duration(0), ttl)
		}
	})
}
