package management

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func (rs *RedisService) GetTTL(ctx context.Context, key string) (time.Duration, error) {
	return rs.operationManager.ExecuteDurationOp(ctx, "TTL", func() (time.Duration, error) {
		return rs.getTTL(ctx, key)
	})
}

func (rs *RedisService) SetTTL(ctx context.Context, key string, ttl time.Duration) error {
	return rs.operationManager.ExecuteWithLock(ctx, "EXPIRE", func() error {
		return rs.setTTL(ctx, key, ttl)
	})
}

func (rs *RedisService) GetBatchTTL(ctx context.Context, keys []string) (map[string]time.Duration, error) {
	return rs.operationManager.ExecuteBatchDurationOp(ctx, "TTL", func() (map[string]time.Duration, error) {
		return rs.getBatchTTL(ctx, keys)
	})
}

func (rs *RedisService) SetBatchTTL(ctx context.Context, keys []string, ttl time.Duration) error {
	return rs.operationManager.ExecuteWithLock(ctx, "EXPIRE", func() error {
		return rs.setBatchTTL(ctx, keys, ttl)
	})
}

func (rs *RedisService) getTTL(ctx context.Context, key string) (time.Duration, error) {
	client := rs.getClient()
	if client == nil {
		return 0, fmt.Errorf("redis client is not initialized")
	}

	finalKey := rs.keyMgr.GetKey(key)
	db, err := rs.keyMgr.GetShardIndex(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get shard index: %v", err)
	}

	// Select appropriate database
	if err := client.Do(ctx, "SELECT", db).Err(); err != nil {
		return 0, fmt.Errorf("failed to select database: %v", err)
	}

	// First check if key exists
	exists, err := client.Exists(ctx, finalKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to check key existence: %v", err)
	}
	if exists == 0 {
		return time.Duration(-2), nil // Key doesn't exist
	}

	ttl, err := client.TTL(ctx, finalKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get TTL: %v", err)
	}

	return ttl, nil
}

func (rs *RedisService) setTTL(ctx context.Context, key string, ttl time.Duration) error {
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

	// Check if key exists
	exists, err := client.Exists(ctx, finalKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check key existence: %v", err)
	}
	if exists == 0 {
		return fmt.Errorf("key does not exist")
	}

	// For zero TTL, use PERSIST command instead of EXPIRE
	if ttl == 0 {
		err = client.Persist(ctx, finalKey).Err()
	} else {
		err = client.Expire(ctx, finalKey, ttl).Err()
	}

	if err != nil {
		return fmt.Errorf("failed to set TTL: %v", err)
	}

	return nil
}

func (rs *RedisService) getBatchTTL(ctx context.Context, keys []string) (map[string]time.Duration, error) {
	result := make(map[string]time.Duration)
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

		cmders, err := pipe.Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to execute pipeline: %v", err)
		}

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
	}

	return result, nil
}

func (rs *RedisService) setBatchTTL(ctx context.Context, keys []string, ttl time.Duration) error {
	if len(keys) == 0 {
		return nil
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
		pipe := rs.client.Pipeline()

		if err := pipe.Do(ctx, "SELECT", db).Err(); err != nil {
			pipe.Close()
			return fmt.Errorf("failed to select database %d: %v", db, err)
		}

		// Add EXISTS commands for all keys
		for _, finalKey := range dbKeys {
			pipe.Exists(ctx, finalKey)
		}

		cmders, err := pipe.Exec(ctx)
		pipe.Close()
		if err != nil {
			return fmt.Errorf("failed to execute pipeline: %v", err)
		}

		// Verify all keys exist
		for i, finalKey := range dbKeys {
			exists, err := cmders[i+1].(*redis.IntCmd).Result() // +1 to skip SELECT
			if err != nil {
				return fmt.Errorf("failed to check key existence: %v", err)
			}
			if exists == 0 {
				return fmt.Errorf("key %s does not exist", keyMapping[finalKey])
			}
		}

		// Second pipeline: Set TTL
		pipe = rs.client.Pipeline()

		if err := pipe.Do(ctx, "SELECT", db).Err(); err != nil {
			pipe.Close()
			return fmt.Errorf("failed to select database %d: %v", db, err)
		}

		for _, finalKey := range dbKeys {
			if ttl == 0 {
				pipe.Persist(ctx, finalKey)
			} else {
				pipe.Expire(ctx, finalKey, ttl)
			}
		}

		_, err = pipe.Exec(ctx)
		pipe.Close()
		if err != nil {
			return fmt.Errorf("failed to execute pipeline: %v", err)
		}
	}

	return nil
}
