// pkg/management/redis.go
package management

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// Modify Get method similarly
func (rs *RedisService) Get(ctx context.Context, key string) (string, error) {
	if rs.cb != nil && rs.cfg.Circuit.Status {
		var result string
		err := rs.cb.Execute(func() error {
			var err error
			result, err = rs.get(ctx, key)
			return err
		})
		return result, err
	}
	return rs.get(ctx, key)
}

// GetBatch retrieves multiple values in a single operation
func (rs *RedisService) GetBatch(ctx context.Context, keys []string) (map[string]string, error) {
	if rs.cb != nil && rs.cfg.Circuit.Status {
		var result map[string]string
		err := rs.cb.Execute(func() error {
			var err error
			result, err = rs.getBatch(ctx, keys)
			return err
		})
		return result, err
	}
	return rs.getBatch(ctx, keys)
}

// Get retrieves a value from Redis by key
func (rs *RedisService) get(ctx context.Context, key string) (string, error) {
	client := rs.getClient()
	if client == nil {
		return "", fmt.Errorf("redis client is not initialized")
	}

	finalKey := rs.keyMgr.GetKey(key)
	db, err := rs.keyMgr.GetShardIndex(key)
	if err != nil {
		return "", fmt.Errorf("failed to get shard index: %v", err)
	}

	// Select appropriate database
	if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
		return "", fmt.Errorf("failed to select database: %v", err)
	}

	// Perform GET operation with retry logic
	var getErr error
	for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
		value, err := client.Get(ctx, finalKey).Result()
		if err == nil {
			return value, nil
		}
		if err == redis.Nil {
			return "", fmt.Errorf("key not found")
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

	return "", fmt.Errorf("failed to get key after %d attempts: %v", rs.cfg.Redis.RetryAttempts, getErr)
}

// getBatch performs the actual batch get operation
func (rs *RedisService) getBatch(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	client := rs.getClient()
	if client == nil {
		return nil, fmt.Errorf("redis client is not initialized")
	}

	// Group keys by database
	keysByDB := make(map[int][]string)
	keyMap := make(map[string]string) // map to store original keys
	for _, key := range keys {
		finalKey := rs.keyMgr.GetKey(key)
		db, err := rs.keyMgr.GetShardIndex(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get shard index for key %s: %v", key, err)
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
			return nil, fmt.Errorf("failed to select database %d: %v", db, err)
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
							return nil, fmt.Errorf("failed to get value: %v", err)
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
				return nil, fmt.Errorf("failed to get keys from database %d after %d attempts: %v",
					db, rs.cfg.Redis.RetryAttempts, getErr)
			}
		}
	}

	return result, nil
}
