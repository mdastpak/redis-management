// pkg/management/redis.go
package management

import (
	"context"
	"fmt"
	"log"
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
	cb        *CircuitBreaker
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

	// Initialize connection
	if err := service.connect(); err != nil {
		return nil, err
	}

	// Initialize pool if enabled
	if cfg.Pool.Status {
		log.Printf("Initializing connection pool with size: %d", cfg.Pool.Size)
		if err := service.initPool(); err != nil {
			return nil, err
		}
	}

	// Initialize circuit breaker if enabled
	if cfg.Circuit.Status {
		log.Printf("Initializing circuit breaker with threshold: %d, reset timeout: %d seconds",
			cfg.Circuit.Threshold, cfg.Circuit.ResetTimeout)
		service.cb = NewCircuitBreaker(
			cfg.Circuit.Threshold,
			time.Duration(cfg.Circuit.ResetTimeout)*time.Second,
			cfg.Circuit.MaxHalfOpen,
		)
	}

	// Initialize bulk processor if enabled
	if cfg.Bulk.Status {
		log.Printf("Starting bulk processor with batch size: %d, flush interval: %d ms", cfg.Bulk.BatchSize, cfg.Bulk.FlushInterval)

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

// getClient returns the appropriate Redis client (pool or regular)
func (rs *RedisService) getClient() *redis.Client {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.cfg.Pool.Status && rs.pool != nil {
		return rs.pool
	}
	return rs.client
}

// Ping checks if Redis is responding
func (rs *RedisService) Ping(ctx context.Context) error {
	client := rs.getClient()
	if client == nil {
		return fmt.Errorf("redis client is not initialized")
	}
	return client.Ping(ctx).Err()
}
