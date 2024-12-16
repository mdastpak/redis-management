package management

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// GetTTL gets TTL for a key
func (rs *RedisService) GetTTL(ctx context.Context, key string) (time.Duration, error) {
	return rs.operationManager.ExecuteDurationOp(ctx, "TTL", func() (time.Duration, error) {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			var result time.Duration
			err := rs.cb.Execute(func() error {
				var ttlErr error
				result, ttlErr = rs.wrapper.WrapTTL(ctx, key, func() (time.Duration, error) {
					return rs.getTTL(ctx, key)
				})
				return ttlErr
			})
			return result, err
		}
		return rs.wrapper.WrapTTL(ctx, key, func() (time.Duration, error) {
			return rs.getTTL(ctx, key)
		})
	})
}

// SetTTL sets TTL for a key
func (rs *RedisService) SetTTL(ctx context.Context, key string, ttl time.Duration) error {
	return rs.operationManager.ExecuteWithLock(ctx, "EXPIRE", func() error {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			return rs.cb.Execute(func() error {
				return rs.wrapper.WrapOperation(ctx, "EXPIRE", map[string]interface{}{
					"key": key,
					"ttl": ttl,
				}, func() error {
					return rs.setTTL(ctx, key, ttl)
				})
			})
		}
		return rs.wrapper.WrapOperation(ctx, "EXPIRE", map[string]interface{}{
			"key": key,
			"ttl": ttl,
		}, func() error {
			return rs.setTTL(ctx, key, ttl)
		})
	})
}

// GetBatchTTL gets TTL for multiple keys
func (rs *RedisService) GetBatchTTL(ctx context.Context, keys []string) (map[string]time.Duration, error) {
	return rs.operationManager.ExecuteBatchDurationOp(ctx, "MTTL", func() (map[string]time.Duration, error) {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			var result map[string]time.Duration
			err := rs.cb.Execute(func() error {
				var ttlErr error
				result, ttlErr = rs.wrapper.WrapBatchTTL(ctx, keys, func() (map[string]time.Duration, error) {
					return rs.getBatchTTL(ctx, keys)
				})
				return ttlErr
			})
			return result, err
		}
		return rs.wrapper.WrapBatchTTL(ctx, keys, func() (map[string]time.Duration, error) {
			return rs.getBatchTTL(ctx, keys)
		})
	})
}

// SetBatchTTL sets TTL for multiple keys
func (rs *RedisService) SetBatchTTL(ctx context.Context, keys []string, ttl time.Duration) error {
	return rs.operationManager.ExecuteWithLock(ctx, "BATCH_EXPIRE", func() error {
		if rs.cb != nil && rs.cfg.Circuit.Status {
			return rs.cb.Execute(func() error {
				return rs.wrapper.WrapOperation(ctx, "BATCH_EXPIRE", map[string]interface{}{
					"keys_count": len(keys),
					"ttl":        ttl,
				}, func() error {
					return rs.setBatchTTL(ctx, keys, ttl)
				})
			})
		}
		return rs.wrapper.WrapOperation(ctx, "BATCH_EXPIRE", map[string]interface{}{
			"keys_count": len(keys),
			"ttl":        ttl,
		}, func() error {
			return rs.setBatchTTL(ctx, keys, ttl)
		})
	})
}

// getTTL performs the actual TTL get operation
func (rs *RedisService) getTTL(ctx context.Context, key string) (time.Duration, error) {
	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("key", key).Error(err.Error())
		return 0, err
	}

	finalKey := rs.keyMgr.GetKey(key)
	db, err := rs.keyMgr.GetShardIndex(key)
	if err != nil {
		return 0, err
	}

	// Select appropriate database
	if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
		return 0, err
	}

	// First check if key exists
	exists, err := client.Exists(ctx, finalKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to check key existence: %v", err)
	}
	if exists == 0 {
		return time.Duration(-2), nil // Key doesn't exist
	}

	// Get TTL with retry logic
	var ttlErr error
	for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
		ttl, err := client.TTL(ctx, finalKey).Result()
		if err == nil {
			return ttl, nil
		}

		ttlErr = err
		if attempt < rs.cfg.Redis.RetryAttempts {
			backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
			if backoff > rs.cfg.Redis.MaxRetryBackoff {
				backoff = rs.cfg.Redis.MaxRetryBackoff
			}
			time.Sleep(backoff)
		}
	}

	return 0, fmt.Errorf("failed to get TTL after %d attempts: %v", rs.cfg.Redis.RetryAttempts, ttlErr)
}

// setTTL performs the actual TTL set operation
func (rs *RedisService) setTTL(ctx context.Context, key string, ttl time.Duration) error {
	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("key", key).Error(err.Error())
		return err
	}

	finalKey := rs.keyMgr.GetKey(key)
	db, err := rs.keyMgr.GetShardIndex(key)
	if err != nil {
		return err
	}

	// Select appropriate database
	if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
		return err
	}

	// Check if key exists
	exists, err := client.Exists(ctx, finalKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check key existence: %v", err)
	}
	if exists == 0 {
		return fmt.Errorf("key does not exist")
	}

	// For zero TTL, use PERSIST command instead of EXPIRE
	var setErr error
	for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
		var err error
		if ttl == 0 {
			err = client.Persist(ctx, finalKey).Err()
		} else {
			err = client.Expire(ctx, finalKey, ttl).Err()
		}

		if err == nil {
			return nil
		}

		setErr = err
		if attempt < rs.cfg.Redis.RetryAttempts {
			backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
			if backoff > rs.cfg.Redis.MaxRetryBackoff {
				backoff = rs.cfg.Redis.MaxRetryBackoff
			}
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed to set TTL: %v", setErr)
}

// getBatchTTL performs the actual batch TTL get operation
func (rs *RedisService) getBatchTTL(ctx context.Context, keys []string) (map[string]time.Duration, error) {
	result := make(map[string]time.Duration)
	if len(keys) == 0 {
		return result, nil
	}

	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("keys_count", len(keys)).Error(err.Error())
		return nil, err
	}

	pipe := rs.client.Pipeline()
	defer pipe.Close()

	keyMapping := make(map[string]string)
	keysByDB := make(map[int][]string)

	// Process keys first
	for _, originalKey := range keys {
		finalKey := rs.keyMgr.GetKey(originalKey)
		keyMapping[finalKey] = originalKey

		db, err := rs.keyMgr.GetShardIndex(originalKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get shard index for key %s: %v", originalKey, err)
		}
		keysByDB[db] = append(keysByDB[db], finalKey)
	}

	// Process each database group
	for db, dbKeys := range keysByDB {
		if err := pipe.Do(ctx, "SELECT", db).Err(); err != nil {
			return nil, fmt.Errorf("failed to select database %d: %v", db, err)
		}

		// First check existence for all keys
		for _, finalKey := range dbKeys {
			pipe.Exists(ctx, finalKey)
		}

		// Then get TTL for all keys
		for _, finalKey := range dbKeys {
			pipe.TTL(ctx, finalKey)
		}

		// Execute pipeline with retry logic
		var ttlErr error
		for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
			cmders, err := pipe.Exec(ctx)
			if err == nil {
				// Process results, skipping the SELECT command result
				cmdIndex := 1 // Skip SELECT command
				numKeys := len(dbKeys)

				// Process EXISTS results
				existsMap := make(map[string]bool)
				for i := 0; i < numKeys; i++ {
					exists, err := cmders[cmdIndex+i].(*redis.IntCmd).Result()
					if err != nil {
						return nil, fmt.Errorf("failed to check key existence: %v", err)
					}
					existsMap[dbKeys[i]] = exists == 1
				}

				// Process TTL results
				cmdIndex += numKeys
				for i := 0; i < numKeys; i++ {
					finalKey := dbKeys[i]
					originalKey := keyMapping[finalKey]

					if !existsMap[finalKey] {
						result[originalKey] = time.Duration(-2) // Key doesn't exist
						continue
					}

					ttl, err := cmders[cmdIndex+i].(*redis.DurationCmd).Result()
					if err != nil {
						return nil, fmt.Errorf("failed to get TTL: %v", err)
					}
					result[originalKey] = ttl
				}
				break
			}

			ttlErr = err
			if attempt < rs.cfg.Redis.RetryAttempts {
				backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
				if backoff > rs.cfg.Redis.MaxRetryBackoff {
					backoff = rs.cfg.Redis.MaxRetryBackoff
				}
				time.Sleep(backoff)
			} else {
				return nil, fmt.Errorf("failed to execute batch TTL operation after %d attempts: %v",
					rs.cfg.Redis.RetryAttempts, ttlErr)
			}
		}
	}

	return result, nil
}

// setBatchTTL performs the actual batch TTL set operation
func (rs *RedisService) setBatchTTL(ctx context.Context, keys []string, ttl time.Duration) error {
	if len(keys) == 0 {
		return nil
	}

	client := rs.getClient()
	if client == nil {
		err := fmt.Errorf("redis client is not initialized")
		rs.logger.WithField("keys_count", len(keys)).Error(err.Error())
		return err
	}

	pipe := rs.client.Pipeline()
	defer pipe.Close()

	keysByDB := make(map[int][]string)

	// Create mapping of original keys to final keys and organize by database
	keyMapping := make(map[string]string) // finalKey -> originalKey
	for _, originalKey := range keys {
		finalKey := rs.keyMgr.GetKey(originalKey)
		keyMapping[finalKey] = originalKey
		db, err := rs.keyMgr.GetShardIndex(originalKey)
		if err != nil {
			return fmt.Errorf("failed to get shard index for key %s: %v", originalKey, err)
		}
		keysByDB[db] = append(keysByDB[db], finalKey)
	}

	for db, dbKeys := range keysByDB {
		// First pipeline: Check existence
		existPipe := rs.client.Pipeline()

		if err := existPipe.Do(ctx, "SELECT", db).Err(); err != nil {
			existPipe.Close()
			return fmt.Errorf("failed to select database %d: %v", db, err)
		}

		// Add EXISTS commands for all keys
		for _, finalKey := range dbKeys {
			existPipe.Exists(ctx, finalKey)
		}

		cmders, err := existPipe.Exec(ctx)
		existPipe.Close()
		if err != nil {
			return fmt.Errorf("failed to execute existence check: %v", err)
		}

		// Verify all keys exist and build list of keys to process
		var keysToProcess []string
		for i, finalKey := range dbKeys {
			exists, err := cmders[i+1].(*redis.IntCmd).Result() // +1 to skip SELECT
			if err != nil {
				return fmt.Errorf("failed to check key existence: %v", err)
			}
			if exists == 0 {
				originalKey := keyMapping[finalKey]
				return fmt.Errorf("key %s does not exist", originalKey)
			}
			keysToProcess = append(keysToProcess, finalKey)
		}

		// Second pipeline: Set TTL
		ttlPipe := rs.client.Pipeline()

		if err := ttlPipe.Do(ctx, "SELECT", db).Err(); err != nil {
			ttlPipe.Close()
			return fmt.Errorf("failed to select database %d: %v", db, err)
		}

		for _, finalKey := range keysToProcess {
			if ttl == 0 {
				ttlPipe.Persist(ctx, finalKey)
			} else {
				ttlPipe.Expire(ctx, finalKey, ttl)
			}
		}

		// Execute pipeline with retry logic
		var setErr error
		for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
			_, err := ttlPipe.Exec(ctx)
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
				ttlPipe.Close()
				return fmt.Errorf("failed to set batch TTL after %d attempts: %v",
					rs.cfg.Redis.RetryAttempts, setErr)
			}
		}
		ttlPipe.Close()
	}

	return nil
}
