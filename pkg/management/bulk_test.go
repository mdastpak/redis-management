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

func TestBulkAndBatchOperations(t *testing.T) {
	t.Parallel()

	t.Run("Batch Operations", func(t *testing.T) {
		t.Run("SetBatch with Default TTL", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

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
	})

	t.Run("Bulk Operations", func(t *testing.T) {
		t.Run("Basic Operation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			// Allow time for bulk processor to initialize
			time.Sleep(100 * time.Millisecond)

			require.NotNil(t, rs.bulkProcessor, "Bulk processor should be initialized")

			err = rs.AddBulkOperation(ctx, "SET", "test_key", "test_value", time.Hour)
			require.NoError(t, err)

			time.Sleep(time.Duration(rs.cfg.Bulk.FlushInterval+1) * time.Second)

			value, err := rs.Get(ctx, "test_key")
			require.NoError(t, err)
			assert.Equal(t, "test_value", value)
		})

		t.Run("Batch Processing", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			batchSize := rs.cfg.Bulk.BatchSize
			for i := 0; i < batchSize; i++ {
				key := fmt.Sprintf("batch_key_%d", i)
				value := fmt.Sprintf("value_%d", i)
				err := rs.AddBulkOperation(ctx, "SET", key, value, time.Hour)
				require.NoError(t, err)
			}

			time.Sleep(time.Duration(rs.cfg.Bulk.FlushInterval+1) * time.Second)

			for i := 0; i < batchSize; i++ {
				key := fmt.Sprintf("batch_key_%d", i)
				expectedValue := fmt.Sprintf("value_%d", i)
				value, err := rs.Get(ctx, key)
				require.NoError(t, err)
				assert.Equal(t, expectedValue, value)
			}
		})

		t.Run("Concurrent Batch Processing", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			var wg sync.WaitGroup
			batchCount := 3
			operationsPerBatch := rs.cfg.Bulk.BatchSize

			for b := 0; b < batchCount; b++ {
				wg.Add(1)
				go func(batchNum int) {
					defer wg.Done()
					for i := 0; i < operationsPerBatch; i++ {
						key := fmt.Sprintf("concurrent_batch_%d_key_%d", batchNum, i)
						value := fmt.Sprintf("value_%d_%d", batchNum, i)
						err := rs.AddBulkOperation(ctx, "SET", key, value, time.Hour)
						require.NoError(t, err)
					}
				}(b)
			}

			wg.Wait()

			time.Sleep(time.Duration(rs.cfg.Bulk.FlushInterval+2) * time.Second)

			for b := 0; b < batchCount; b++ {
				for i := 0; i < operationsPerBatch; i++ {
					key := fmt.Sprintf("concurrent_batch_%d_key_%d", b, i)
					expectedValue := fmt.Sprintf("value_%d_%d", b, i)
					value, err := rs.Get(ctx, key)
					require.NoError(t, err)
					assert.Equal(t, expectedValue, value)
				}
			}
		})
	})

	t.Run("Bulk Operations Status", func(t *testing.T) {
		t.Run("Metrics and Status", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			operationCount := 5
			for i := 0; i < operationCount; i++ {
				err := rs.AddBulkOperation(ctx, "SET", fmt.Sprintf("metrics_key_%d", i), "value", time.Hour)
				require.NoError(t, err)
			}

			time.Sleep(time.Duration(rs.cfg.Bulk.FlushInterval+1) * time.Second)

			status := rs.bulkProcessor.GetStatus()
			assert.True(t, status.IsRunning)
			assert.Greater(t, status.AverageLatency, time.Duration(0))
			assert.Less(t, status.ErrorRate, float64(1))
		})

		t.Run("Error Handling", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			// First stop the bulk processor
			if rs.bulkProcessor != nil {
				rs.bulkProcessor.Stop()
			}

			// Then close the client
			err = rs.client.Close()
			require.NoError(t, err)

			// Now try operations - they should fail gracefully
			operationCount := 5
			errors := make([]error, operationCount)
			for i := 0; i < operationCount; i++ {
				errors[i] = rs.AddBulkOperation(ctx, "SET", fmt.Sprintf("error_key_%d", i), "value", time.Hour)
			}

			// Verify errors
			for _, err := range errors {
				assert.Error(t, err)
			}

			// Cleanup
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = rs.Close(closeCtx)
		})

		t.Run("Shutdown Behavior", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			for i := 0; i < 5; i++ {
				err := rs.AddBulkOperation(ctx, "SET", fmt.Sprintf("shutdown_key_%d", i), "value", time.Hour)
				require.NoError(t, err)
			}

			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			err = rs.Close(closeCtx)
			require.NoError(t, err)

			status := rs.bulkProcessor.GetStatus()
			assert.False(t, status.IsRunning)

			err = rs.AddBulkOperation(ctx, "SET", "post_shutdown_key", "value", time.Hour)
			assert.Error(t, err)
		})
	})

	t.Run("Edge Cases", func(t *testing.T) {

		t.Run("Context Cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			cancel()
			err = rs.AddBulkOperation(ctx, "SET", "cancelled_key", "value", time.Hour)
			assert.Error(t, err)
			assert.Equal(t, context.Canceled, err)
		})

		t.Run("Large Batch Processing", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Enable bulk operations in config
			newCfg := *rs.cfg
			newCfg.Bulk.Status = true
			newCfg.Bulk.BatchSize = 10
			newCfg.Bulk.FlushInterval = 1
			newCfg.Bulk.MaxRetries = 3
			newCfg.Bulk.ConcurrentFlush = true

			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			operationCount := rs.cfg.Bulk.BatchSize * 3
			for i := 0; i < operationCount; i++ {
				err := rs.AddBulkOperation(ctx, "SET", fmt.Sprintf("large_batch_key_%d", i), "value", time.Hour)
				require.NoError(t, err)
			}

			time.Sleep(time.Duration(rs.cfg.Bulk.FlushInterval+3) * time.Second)

			status := rs.bulkProcessor.GetStatus()
			assert.True(t, status.IsRunning)
			assert.Greater(t, status.AverageLatency, time.Duration(0))
		})
	})
}
