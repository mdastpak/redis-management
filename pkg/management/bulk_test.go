package management

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkOperationsScalability(t *testing.T) {
	t.Parallel()

	scales := []int{1, 10, 100, 1000}

	for _, scale := range scales {
		t.Run(fmt.Sprintf("Scale_%dx", scale), func(t *testing.T) {
			rs, ctx, cancel := setupTestRedisWithConfig(t,
				WithScale(scale),
			)
			defer cancel()

			// Prepare test data
			items := make(map[string]interface{}, scale*10)
			for i := 0; i < scale*10; i++ {
				key := fmt.Sprintf("bulk_key_%d", i)
				items[key] = fmt.Sprintf("value_%d", i)
			}

			// Execute bulk operation
			start := time.Now()
			err := rs.BulkSet(ctx, items, time.Hour)
			require.NoError(t, err)
			elapsed := time.Since(start)

			// Verify results
			sampleSize := min(len(items), 10)
			sampleKeys := make([]string, 0, sampleSize)
			i := 0
			for k := range items {
				if i >= sampleSize {
					break
				}
				sampleKeys = append(sampleKeys, k)
				i++
			}

			for _, key := range sampleKeys {
				value, err := rs.Get(ctx, key)
				require.NoError(t, err)
				assert.Equal(t, items[key], value)

				ttl, err := rs.GetTTL(ctx, key)
				require.NoError(t, err)
				assert.True(t, ttl > 0 && ttl <= time.Hour)
			}

			// Log performance metrics
			metrics := rs.GetBulkMetrics()
			t.Logf("Scale %dx Performance:", scale)
			t.Logf("- Items processed: %d", len(items))
			t.Logf("- Total time: %v", elapsed)
			t.Logf("- Items/second: %.2f", float64(len(items))/elapsed.Seconds())
			t.Logf("- Success rate: %.2f%%", float64(metrics.SuccessCount.Load())/float64(len(items))*100)
		})
	}
}

func TestBulkOperations(t *testing.T) {
	t.Parallel()

	t.Run("Basic Bulk Set", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		items := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		err := rs.BulkSet(ctx, items, time.Hour)
		require.NoError(t, err)

		// Verify all items
		for key, expected := range items {
			value, err := rs.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expected, value)

			ttl, err := rs.GetTTL(ctx, key)
			require.NoError(t, err)
			assert.True(t, ttl > 0 && ttl <= time.Hour)
		}
	})

	t.Run("Bulk Delete", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// First set some keys
		items := map[string]interface{}{
			"del_key1": "value1",
			"del_key2": "value2",
		}
		err := rs.BulkSet(ctx, items, time.Hour)
		require.NoError(t, err)

		// Then delete them
		keys := []string{"del_key1", "del_key2"}
		err = rs.BulkDelete(ctx, keys)
		require.NoError(t, err)

		// Verify deletion
		for _, key := range keys {
			_, err := rs.Get(ctx, key)
			assert.Error(t, err, "Key should be deleted")
		}
	})

	t.Run("Bulk SetTTL", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// First set some keys
		items := map[string]interface{}{
			"ttl_key1": "value1",
			"ttl_key2": "value2",
		}
		err := rs.BulkSet(ctx, items, time.Hour)
		require.NoError(t, err)

		// Update TTL
		newTTL := 30 * time.Minute
		keys := []string{"ttl_key1", "ttl_key2"}
		err = rs.BulkSetTTL(ctx, keys, newTTL)
		require.NoError(t, err)

		// Verify new TTL
		for _, key := range keys {
			ttl, err := rs.GetTTL(ctx, key)
			require.NoError(t, err)
			assert.True(t, ttl > 0 && ttl <= newTTL)
		}
	})
}
