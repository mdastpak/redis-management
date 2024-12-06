// pkg/management/redis.go
package management

import (
	"context"
	"fmt"
	"time"
)

// Delete method with circuit breaker support
func (rs *RedisService) Delete(ctx context.Context, key string) error {
	return rs.operationManager.ExecuteWithLock(ctx, "DEL", func() error {

		if rs.cb != nil && rs.cfg.Circuit.Status {
			return rs.cb.Execute(func() error {
				return rs.delete(ctx, key)
			})
		}
		return rs.delete(ctx, key)
	})
}

// delete performs the actual delete operation with retries and bulk support
func (rs *RedisService) delete(ctx context.Context, key string) error {
	// Check if bulk operations are enabled
	if rs.cfg.Bulk.Status {
		return rs.AddBulkOperation(ctx, "DEL", key, nil, 0)
	}

	client := rs.getClient()
	if client == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	finalKey := rs.keyMgr.GetKey(key)
	db, err := rs.keyMgr.GetShardIndex(key)
	if err != nil {
		return fmt.Errorf("failed to get shard index: %v", err)
	}

	// Select appropriate database
	if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
		return fmt.Errorf("failed to select database: %v", err)
	}

	// Perform DELETE operation with retry logic
	var delErr error
	for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
		err := client.Del(ctx, finalKey).Err()
		if err == nil {
			return nil
		}

		delErr = err
		if attempt < rs.cfg.Redis.RetryAttempts {
			// Calculate backoff time
			backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
			if backoff > rs.cfg.Redis.MaxRetryBackoff {
				backoff = rs.cfg.Redis.MaxRetryBackoff
			}
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed to delete key after %d attempts: %v",
		rs.cfg.Redis.RetryAttempts, delErr)
}

// DeleteBatch removes multiple keys in a single operation
func (rs *RedisService) DeleteBatch(ctx context.Context, keys []string) error {
	if rs.cb != nil && rs.cfg.Circuit.Status {
		return rs.cb.Execute(func() error {
			return rs.deleteBatch(ctx, keys)
		})
	}
	return rs.deleteBatch(ctx, keys)
}

// deleteBatch performs the actual batch delete operation
func (rs *RedisService) deleteBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	client := rs.getClient()
	if client == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	// Group keys by database for efficiency
	keysByDB := make(map[int][]string)
	for _, key := range keys {
		finalKey := rs.keyMgr.GetKey(key)
		db, err := rs.keyMgr.GetShardIndex(key)
		if err != nil {
			return fmt.Errorf("failed to get shard index for key %s: %v", key, err)
		}
		keysByDB[db] = append(keysByDB[db], finalKey)
	}

	// Process each database group
	for db, dbKeys := range keysByDB {
		// Select database
		if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
			return fmt.Errorf("failed to select database %d: %v", db, err)
		}

		// Perform batch delete with retry logic
		var delErr error
		for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
			err := client.Del(ctx, dbKeys...).Err()
			if err == nil {
				break
			}

			delErr = err
			if attempt < rs.cfg.Redis.RetryAttempts {
				backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
				if backoff > rs.cfg.Redis.MaxRetryBackoff {
					backoff = rs.cfg.Redis.MaxRetryBackoff
				}
				time.Sleep(backoff)
			} else {
				return fmt.Errorf("failed to delete keys in database %d after %d attempts: %v",
					db, rs.cfg.Redis.RetryAttempts, delErr)
			}
		}
	}

	return nil
}
