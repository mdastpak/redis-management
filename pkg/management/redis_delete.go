// pkg/management/redis.go
package management

import (
	"context"
	"fmt"
	"time"
)

// Delete removes a key from Redis
func (rs *RedisService) Delete(ctx context.Context, key string) error {
	return rs.operationManager.ExecuteWithLock(ctx, "DEL", func() error {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			return rs.cb.Execute(func() error {
				return rs.wrapper.WrapDelete(ctx, key, func() error {
					return rs.delete(ctx, key)
				})
			})
		}
		return rs.wrapper.WrapDelete(ctx, key, func() error {
			return rs.delete(ctx, key)
		})
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
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("key", key).Error(err.Error())
		return err
	}

	finalKey := rs.keyMgr.GetKey(key)
	db, err := rs.keyMgr.GetShardIndex(key)
	if err != nil {
		err := fmt.Errorf("failed to get shard index: %v", err)
		rs.logger.WithField("key", key).Error(err.Error())
		return err
	}

	// Select appropriate database
	if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
		err := fmt.Errorf("failed to select database: %v", err)
		rs.logger.WithField("key", key).Error(err.Error())
		return err
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

	err = fmt.Errorf("failed to delete key after %d attempts: %v",
		rs.cfg.Redis.RetryAttempts, delErr)
	rs.logger.WithField("key", key).Error(err.Error())
	return err
}

// DeleteBatch removes multiple keys in a single operation
func (rs *RedisService) DeleteBatch(ctx context.Context, keys []string) error {
	return rs.operationManager.ExecuteWithLock(ctx, "MDEL", func() error {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			return rs.cb.Execute(func() error {
				return rs.wrapper.WrapBatchDelete(ctx, keys, func() error {
					return rs.deleteBatch(ctx, keys)
				})
			})
		}
		return rs.wrapper.WrapBatchDelete(ctx, keys, func() error {
			return rs.deleteBatch(ctx, keys)
		})
	})
}

// deleteBatch performs the actual batch delete operation
func (rs *RedisService) deleteBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("keys", keys).Error(err.Error())
		return err
	}

	// Group keys by database for efficiency
	keysByDB := make(map[int][]string)
	for _, key := range keys {
		finalKey := rs.keyMgr.GetKey(key)
		db, err := rs.keyMgr.GetShardIndex(key)
		if err != nil {
			err := fmt.Errorf("failed to get shard index for key %s: %v", key, err)
			rs.logger.WithField("key", key).Error(err.Error())
			return err
		}
		keysByDB[db] = append(keysByDB[db], finalKey)
	}

	// Process each database group
	for db, dbKeys := range keysByDB {
		// Select database
		if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
			err := fmt.Errorf("failed to select database %d: %v", db, err)
			rs.logger.WithField("db", db).Error(err.Error())
			return err
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
				err = fmt.Errorf("failed to delete keys in database %d after %d attempts: %v",
					db, rs.cfg.Redis.RetryAttempts, delErr)
				rs.logger.WithField("keys", dbKeys).Error(err.Error())
				return err
			}
		}
	}

	return nil
}
