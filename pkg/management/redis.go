// pkg/management/redis.go
package management

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mdastpak/redis-management/config"

	"github.com/go-redis/redis/v8"
)

type RedisService struct {
	cfg       *config.Config
	client    *redis.Client
	pool      *redis.Client
	keyMgr    *KeyManager
	bulkQueue chan BulkOperation
	mu        sync.RWMutex
}

func NewRedisService(cfg *config.Config) (*RedisService, error) {
	keyMgr, err := NewKeyManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create key manager: %v", err)
	}

	service := &RedisService{
		cfg:       cfg,
		keyMgr:    keyMgr,
		bulkQueue: make(chan BulkOperation, cfg.Bulk.BatchSize),
	}

	if err := service.connect(); err != nil {
		return nil, err
	}

	if cfg.Pool.Status {
		if err := service.initPool(); err != nil {
			return nil, err
		}
	}

	if cfg.Bulk.Status {
		service.startBulkProcessor()
	}

	return service, nil
}

func (rs *RedisService) connect() error {
	options := &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Port),
		Password:     rs.cfg.Redis.Password,
		DialTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		ReadTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		WriteTimeout: time.Duration(rs.cfg.Redis.Timeout) * time.Second,
	}

	rs.client = redis.NewClient(options)
	return rs.client.Ping(context.Background()).Err()
}

func (rs *RedisService) Close() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	var errs []error
	if rs.client != nil {
		if err := rs.client.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if rs.pool != nil {
		if err := rs.pool.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	return nil
}

func (rs *RedisService) startBulkProcessor() {
	processor := NewBulkProcessor(rs)
	processor.Start(context.Background())
}

// Add bulk operation methods
func (rs *RedisService) AddBulkOperation(ctx context.Context, command string, key string, value interface{}, expires time.Duration) error {
	result := make(chan error, 1)

	select {
	case rs.bulkQueue <- BulkOperation{
		Command:   command,
		Key:       key,
		Value:     value,
		ExpiresAt: expires,
		Result:    result,
	}:
		return <-result
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Set stores a key-value pair in Redis with an expiration time
func (rs *RedisService) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if rs.cfg.Bulk.Status {
		return rs.AddBulkOperation(ctx, "SET", key, value, expiration)
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

	// Perform SET operation with retry logic
	var setErr error
	for attempt := 0; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
		err := client.Set(ctx, finalKey, value, expiration).Err()
		if err == nil {
			return nil
		}

		setErr = err
		if attempt < rs.cfg.Redis.RetryAttempts {
			// Calculate backoff time
			backoff := time.Duration(attempt+1) * rs.cfg.Redis.RetryDelay
			if backoff > rs.cfg.Redis.MaxRetryBackoff {
				backoff = rs.cfg.Redis.MaxRetryBackoff
			}
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed to set key after %d attempts: %v", rs.cfg.Redis.RetryAttempts, setErr)
}

// Get retrieves a value from Redis by key
func (rs *RedisService) Get(ctx context.Context, key string) (string, error) {
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

// getClient returns the appropriate Redis client (pool or regular)
func (rs *RedisService) getClient() *redis.Client {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.cfg.Pool.Status && rs.pool != nil {
		return rs.pool
	}
	return rs.client
}
