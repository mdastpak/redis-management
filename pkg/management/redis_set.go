package management

import (
	"context"
	"fmt"
	"time"
)

// Set stores a key-value pair in Redis with expiration time
func (rs *RedisService) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return rs.operationManager.ExecuteWithLock(ctx, "SET", func() error {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			return rs.cb.Execute(func() error {
				return rs.wrapper.WrapSet(ctx, key, value, func() error {
					return rs.set(ctx, key, value, expiration)
				})
			})
		}
		return rs.wrapper.WrapSet(ctx, key, value, func() error {
			return rs.set(ctx, key, value, expiration)
		})
	})
}

// SetWithDefaultTTL stores a value with configured default TTL
func (rs *RedisService) SetWithDefaultTTL(ctx context.Context, key string, value interface{}) error {
	return rs.Set(ctx, key, value, rs.cfg.Redis.TTL)
}

// SetBatch stores multiple key-value pairs in a single operation
func (rs *RedisService) SetBatch(ctx context.Context, items map[string]interface{}, expiration time.Duration) error {
	return rs.operationManager.ExecuteWithLock(ctx, "MSET", func() error {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			return rs.cb.Execute(func() error {
				return rs.wrapper.WrapBatchSet(ctx, items, func() error {
					return rs.setBatch(ctx, items, expiration)
				})
			})
		}
		return rs.wrapper.WrapBatchSet(ctx, items, func() error {
			return rs.setBatch(ctx, items, expiration)
		})
	})
}

// SetBatchWithDefaultTTL stores multiple values with configured default TTL
func (rs *RedisService) SetBatchWithDefaultTTL(ctx context.Context, items map[string]interface{}) error {
	return rs.SetBatch(ctx, items, rs.cfg.Redis.TTL)
}

// Set stores a key-value pair in Redis with an expiration time
func (rs *RedisService) set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if rs.cfg.Bulk.Status {
		return rs.AddBulkOperation(ctx, "SET", key, value, expiration)
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

	// Perform SET operation with retry logic
	var setErr error
	for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
		if err := client.Set(ctx, finalKey, value, expiration).Err(); err != nil {
			setErr = err
			if attempt < rs.cfg.Redis.RetryAttempts {
				// Calculate backoff time
				backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
				if backoff > rs.cfg.Redis.MaxRetryBackoff {
					backoff = rs.cfg.Redis.MaxRetryBackoff
				}
				time.Sleep(backoff)
				continue
			}
			err := fmt.Errorf("failed to set key after %d attempts: %v", rs.cfg.Redis.RetryAttempts, setErr)
			rs.logger.WithField("key", key).Error(err.Error())
			return err
		}
		break
	}

	return nil
}

// setBatch performs the actual batch set operation
func (rs *RedisService) setBatch(ctx context.Context, items map[string]interface{}, expiration time.Duration) error {
	if len(items) == 0 {
		return nil
	}

	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("items", items).Error(err.Error())
		return err
	}

	// Group items by database for efficiency
	itemsByDB := make(map[int]map[string]interface{})
	for key, value := range items {
		finalKey := rs.keyMgr.GetKey(key)
		db, err := rs.keyMgr.GetShardIndex(key)
		if err != nil {
			err := fmt.Errorf("failed to get shard index for key %s: %v", key, err)
			rs.logger.WithField("key", key).Error(err.Error())
			return err
		}
		if itemsByDB[db] == nil {
			itemsByDB[db] = make(map[string]interface{})
		}
		itemsByDB[db][finalKey] = value
	}

	// Process each database group
	pipe := client.Pipeline()
	defer pipe.Close()

	for db, dbItems := range itemsByDB {
		// Select database
		if err := pipe.Do(ctx, "SELECT", db).Err(); err != nil {
			err := fmt.Errorf("failed to select database %d: %v", db, err)
			rs.logger.WithField("db", db).Error(err.Error())
			return err
		}

		// Add all items to pipeline
		for key, value := range dbItems {
			pipe.Set(ctx, key, value, expiration)
		}

		// Execute pipeline with retry logic
		var setErr error
		for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
			_, err := pipe.Exec(ctx)
			if err == nil {
				break
			}

			setErr = err
			if attempt < rs.cfg.Redis.RetryAttempts {
				backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
				if backoff > rs.cfg.Redis.MaxRetryBackoff {
					backoff = rs.cfg.Redis.MaxRetryBackoff
				}
				time.Sleep(backoff)
			} else {
				err := fmt.Errorf("failed to set keys in database %d after %d attempts: %v",
					db, rs.cfg.Redis.RetryAttempts, setErr)
				rs.logger.WithField("db", db).Error(err.Error())
				return err
			}
		}
	}

	return nil
}
