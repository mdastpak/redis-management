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

		// Verify values and TTLs
		for key, expectedValue := range items {
			value, err := mr.Get(key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)

			ttl := mr.TTL(key)
			assert.True(t, ttl > 110*time.Minute,
				"TTL should be close to default 2 hours for key %s", key)
		}
	})
}
