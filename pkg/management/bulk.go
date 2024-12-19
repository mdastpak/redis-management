// bulk.go
package management

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// BulkResult represents the result of a bulk operation
type BulkResult struct {
	SuccessCount int
	FailedKeys   []string
	Errors       []error
}

// BulkMetrics holds statistics for bulk operations
type BulkMetrics struct {
	OperationsCount   atomic.Int64
	SuccessCount      atomic.Int64
	ErrorCount        atomic.Int64
	LastOperationTime atomic.Value // stores time.Time
	AverageLatency    atomic.Value // stores time.Duration
}

// BulkSet performs a bulk SET operation for multiple key-value pairs
func (rs *RedisService) BulkSet(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	return rs.executeBulkOperation(ctx, "BULK_SET", len(items), func(pipe redis.Pipeliner) error {
		for key, value := range items {
			finalKey := rs.keyMgr.GetKey(key)
			pipe.Set(ctx, finalKey, value, ttl)
		}
		return nil
	})
}

// BulkDelete performs a bulk DELETE operation for multiple keys
func (rs *RedisService) BulkDelete(ctx context.Context, keys []string) error {
	return rs.executeBulkOperation(ctx, "BULK_DELETE", len(keys), func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			finalKey := rs.keyMgr.GetKey(key)
			pipe.Del(ctx, finalKey)
		}
		return nil
	})
}

// BulkSetTTL performs a bulk EXPIRE operation for multiple keys
func (rs *RedisService) BulkSetTTL(ctx context.Context, keys []string, ttl time.Duration) error {
	return rs.executeBulkOperation(ctx, "BULK_SETTTL", len(keys), func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			finalKey := rs.keyMgr.GetKey(key)
			pipe.Expire(ctx, finalKey, ttl)
		}
		return nil
	})
}

// executeBulkOperation handles execution of bulk operations using Redis pipeline
func (rs *RedisService) executeBulkOperation(ctx context.Context, operation string, itemCount int,
	fn func(redis.Pipeliner) error) error {

	startTime := time.Now()
	result := &BulkResult{}

	err := rs.operationManager.ExecuteWithLock(ctx, operation, func() error {
		if rs.client == nil {
			return fmt.Errorf("redis client is not initialized")
		}

		pipe := rs.client.Pipeline()
		defer pipe.Close()

		if err := fn(pipe); err != nil {
			return fmt.Errorf("failed to prepare bulk operation: %w", err)
		}

		cmds, err := pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to execute bulk operation: %w", err)
		}

		// Process results
		for i, cmd := range cmds {
			if err := cmd.Err(); err != nil {
				result.Errors = append(result.Errors, err)
				result.FailedKeys = append(result.FailedKeys, fmt.Sprintf("%s_%d", operation, i))
			} else {
				result.SuccessCount++
			}
		}

		return nil
	})

	// Update metrics regardless of operation result
	rs.updateBulkMetrics(result, time.Since(startTime))

	return err
}

// updateBulkMetrics updates operation metrics
func (rs *RedisService) updateBulkMetrics(result *BulkResult, duration time.Duration) {
	rs.metrics.OperationsCount.Add(1)
	rs.metrics.SuccessCount.Add(int64(result.SuccessCount))
	rs.metrics.ErrorCount.Add(int64(len(result.Errors)))
	rs.metrics.LastOperationTime.Store(time.Now())

	// Update average latency
	currentLatency, _ := rs.metrics.AverageLatency.Load().(time.Duration)
	totalOps := rs.metrics.OperationsCount.Load()
	newLatency := (currentLatency*time.Duration(totalOps-1) + duration) / time.Duration(totalOps)
	rs.metrics.AverageLatency.Store(newLatency)
}

// GetBulkMetrics returns the current bulk operation metrics
func (rs *RedisService) GetBulkMetrics() BulkMetrics {
	var metrics BulkMetrics
	metrics.OperationsCount.Store(rs.metrics.OperationsCount.Load())
	metrics.SuccessCount.Store(rs.metrics.SuccessCount.Load())
	metrics.ErrorCount.Store(rs.metrics.ErrorCount.Load())
	metrics.LastOperationTime.Store(rs.metrics.LastOperationTime.Load())
	metrics.AverageLatency.Store(rs.metrics.AverageLatency.Load())
	return metrics
}
