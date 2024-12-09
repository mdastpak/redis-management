package management

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchOperations(t *testing.T) {
	t.Parallel()

	t.Run("SetBatch with Default TTL", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		items := map[string]interface{}{
			"batch_key1": "value1",
			"batch_key2": "value2",
		}

		err = rs.SetBatchWithDefaultTTL(ctx, items)
		require.NoError(t, err)

		for key, expectedValue := range items {
			value, err := rs.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)

			ttl, err := rs.GetTTL(ctx, key)
			require.NoError(t, err)
			t.Logf("TTL for key %s: %v", key, ttl)
			assert.Equal(t, time.Duration(rs.cfg.Redis.TTL), ttl)
		}
	})
}
