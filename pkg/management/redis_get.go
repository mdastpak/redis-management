// pkg/management/redis.go
package management

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// Get retrieves a value by key
func (rs *RedisService) Get(ctx context.Context, key string) (string, error) {
	return rs.operationManager.ExecuteReadOp(ctx, "GET", func() (string, error) {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			var result string
			err := rs.cb.Execute(func() error {
				var getErr error
				result, getErr = rs.wrapper.WrapGet(ctx, key, func() (string, error) {
					return rs.get(ctx, key)
				})
				return getErr
			})
			return result, err
		}
		return rs.wrapper.WrapGet(ctx, key, func() (string, error) {
			return rs.get(ctx, key)
		})
	})
}

// GetBatch retrieves multiple values in a single operation
func (rs *RedisService) GetBatch(ctx context.Context, keys []string) (map[string]string, error) {
	return rs.operationManager.ExecuteBatchOp(ctx, "MGET", func() (map[string]string, error) {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			var result map[string]string
			err := rs.cb.Execute(func() error {
				var getBatchErr error
				result, getBatchErr = rs.wrapper.WrapBatchGet(ctx, keys, func() (map[string]string, error) {
					return rs.getBatch(ctx, keys)
				})
				return getBatchErr
			})
			return result, err
		}
		return rs.wrapper.WrapBatchGet(ctx, keys, func() (map[string]string, error) {
			return rs.getBatch(ctx, keys)
		})
	})
}

// Get retrieves a value from Redis by key
func (rs *RedisService) get(ctx context.Context, key string) (string, error) {
	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("key", key).Error(err.Error())
		return "", err
	}

	finalKey := rs.keyMgr.GetKey(key)
	db, err := rs.keyMgr.GetShardIndex(key)
	if err != nil {
		err := fmt.Errorf("failed to get shard index: %v", err)
		rs.logger.WithField("key", key).Error(err.Error())
		return "", err
	}

	// Select appropriate database
	if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
		err := fmt.Errorf("failed to select database: %v", err)
		rs.logger.WithField("key", key).Error(err.Error())
		return "", err
	}

	// Perform GET operation with retry logic
	var getErr error
	for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
		value, err := client.Get(ctx, finalKey).Result()
		if err == nil {
			return value, nil
		}
		if err == redis.Nil {
			err = fmt.Errorf("key not found")
			rs.logger.WithField("key", key).Warn(err.Error())
			return "", err
		}

		getErr = err
		if attempt < rs.cfg.Redis.RetryAttempts {
			// Calculate backoff time
			backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
			if backoff > rs.cfg.Redis.MaxRetryBackoff {
				backoff = rs.cfg.Redis.MaxRetryBackoff
			}
			time.Sleep(backoff)
		}
	}

	// Log error and return
	err = fmt.Errorf("failed to get key after %d attempts: %v", rs.cfg.Redis.RetryAttempts, getErr)
	rs.logger.WithField("key", key).Error(err.Error())
	return "", err
}

// getBatch performs the actual batch get operation
func (rs *RedisService) getBatch(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("keys", keys).Error(err.Error())
		return nil, err
	}

	// Group keys by database
	keysByDB := make(map[int][]string)
	keyMap := make(map[string]string) // map to store original keys
	for _, key := range keys {
		finalKey := rs.keyMgr.GetKey(key)
		db, err := rs.keyMgr.GetShardIndex(key)
		if err != nil {
			err := fmt.Errorf("failed to get shard index for key %s: %v", key, err)
			rs.logger.WithField("key", key).Error(err.Error())
			return nil, err
		}
		keysByDB[db] = append(keysByDB[db], finalKey)
		keyMap[finalKey] = key // store mapping of final key to original key
	}

	// Process each database group
	result := make(map[string]string)
	pipe := client.Pipeline()
	defer pipe.Close()

	for db, dbKeys := range keysByDB {
		// Select database
		if err := pipe.Do(ctx, "SELECT", db).Err(); err != nil {
			err := fmt.Errorf("failed to select database %d: %v", db, err)
			rs.logger.WithField("db", db).Error(err.Error())
			return nil, err
		}

		// Add all keys to pipeline
		for _, key := range dbKeys {
			pipe.Get(ctx, key)
		}

		// Execute pipeline with retry logic
		var getErr error
		for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
			cmders, err := pipe.Exec(ctx)
			if err == nil {
				// Process results
				for i, cmder := range cmders {
					if i == 0 { // Skip SELECT command result
						continue
					}
					// Type assert to *redis.StringCmd
					if get, ok := cmder.(*redis.StringCmd); ok {
						val, err := get.Result()
						if err == redis.Nil {
							continue // Skip not found keys
						}
						if err != nil {
							err := fmt.Errorf("failed to get value: %v", err)
							rs.logger.WithField("key", dbKeys[i-1]).Error(err.Error())
							return nil, err
						}
						finalKey := dbKeys[i-1]
						originalKey := keyMap[finalKey]
						result[originalKey] = val
					}
				}
				break
			}

			getErr = err
			if attempt < rs.cfg.Redis.RetryAttempts {
				backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
				if backoff > rs.cfg.Redis.MaxRetryBackoff {
					backoff = rs.cfg.Redis.MaxRetryBackoff
				}
				time.Sleep(backoff)
			} else {
				err := fmt.Errorf("failed to get keys from database %d after %d attempts: %v",
					db, rs.cfg.Redis.RetryAttempts, getErr)
				rs.logger.WithField("db", db).Error(err.Error())
				return nil, err
			}
		}
	}

	return result, nil
}
