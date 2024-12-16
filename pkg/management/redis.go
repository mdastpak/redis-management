package management

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mdastpak/redis-management/config"
	"github.com/mdastpak/redis-management/pkg/logging"

	"github.com/go-redis/redis/v8"
)

type RedisService struct {
	cfg              *config.Config
	client           *redis.Client
	pool             *redis.Client
	poolManager      *PoolManager
	keyMgr           *KeyManager
	bulkQueue        chan BulkOperation
	mu               sync.RWMutex
	cb               *CircuitBreaker
	operationManager *OperationManager
	wrapper          *OperationWrapper
	logger           logging.Logger
}

func NewRedisService(cfg *config.Config) (*RedisService, error) {
	// Initialize logger
	logger := logging.NewLogger(
		logging.WithLevel(logging.LevelFromConfig(cfg.Logging.Level)),
		logging.WithFormatter(logging.NewFormatter(logging.FormatFromString(cfg.Logging.Format))),
	)

	keyMgr, err := NewKeyManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create key manager: %v", err)
	}

	service := &RedisService{
		cfg:       cfg,
		keyMgr:    keyMgr,
		bulkQueue: make(chan BulkOperation, cfg.Bulk.BatchSize),
		logger:    logger,
	}

	// Create wrapper after service is initialized
	service.wrapper = NewOperationWrapper(service, logger)

	// Initialize operation manager
	service.operationManager, err = NewOperationManager(service)
	if err != nil {
		return nil, fmt.Errorf("failed to create operation manager: %v", err)
	}

	// Initialize connection
	if err := service.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	// Initialize pool if enabled
	if cfg.Pool.Status {
		// log.Printf("Initializing connection pool with size: %d", cfg.Pool.Size)
		service.logger.WithFields(map[string]interface{}{
			"pool_size":    cfg.Pool.Size,
			"min_idle":     cfg.Pool.MinIdle,
			"wait_timeout": cfg.Pool.WaitTimeout,
		}).Info("Initializing connection pool")

		if err := service.NewPoolManager(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to create pool manager: %v", err)
		}
	}

	// Initialize circuit breaker if enabled
	if cfg.Circuit.Status {
		// log.Printf("Initializing circuit breaker with threshold: %d, reset timeout: %d seconds", cfg.Circuit.Threshold, cfg.Circuit.ResetTimeout)
		service.logger.WithFields(map[string]interface{}{
			"threshold":     cfg.Circuit.Threshold,
			"reset_timeout": cfg.Circuit.ResetTimeout,
		}).Info("Initializing circuit breaker")

		service.cb = NewCircuitBreaker(
			cfg.Circuit.Threshold,
			time.Duration(cfg.Circuit.ResetTimeout)*time.Second,
			cfg.Circuit.MaxHalfOpen,
		)
	}

	// Initialize bulk processor if enabled
	if cfg.Bulk.Status {
		// log.Printf("Starting bulk processor with batch size: %d, flush interval: %d ms", cfg.Bulk.BatchSize, cfg.Bulk.FlushInterval)
		service.logger.WithFields(map[string]interface{}{
			"batch_size":     cfg.Bulk.BatchSize,
			"flush_interval": cfg.Bulk.FlushInterval,
		}).Info("Starting bulk processor")

		service.startBulkProcessor()
	}

	service.logger.Info("Redis service initialized successfully")
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

	rs.logger.WithFields(map[string]interface{}{
		"host":    rs.cfg.Redis.Host,
		"port":    rs.cfg.Redis.Port,
		"timeout": rs.cfg.Redis.Timeout,
	}).Debug("Connecting to Redis")

	rs.client = redis.NewClient(options)
	if err := rs.client.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("failed to ping Redis: %v", err)
	}

	// rs.logger.Info("Successfully connected to Redis")
	return nil
}

func (rs *RedisService) Close(ctx context.Context) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.logger.Info("Starting service shutdown")

	var errs []error

	// Attempt graceful shutdown first
	if rs.operationManager != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, rs.cfg.Redis.ShutdownTimeout)
		defer cancel()

		if err := rs.operationManager.GetShutdownManager().Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("graceful shutdown failed: %v", err))
		}
	}

	// Close client connections
	if rs.client != nil {
		rs.logger.Debug("Closing main Redis client")
		if err := rs.client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing client: %v", err))
		}
		rs.client = nil
	}

	// Close connection pool
	if rs.pool != nil {
		rs.logger.Debug("Closing Redis connection pool")
		if err := rs.pool.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing pool: %v", err))
		}
		rs.pool = nil
	}

	if len(errs) > 0 {
		for _, err := range errs {
			rs.logger.WithError(err).Error("Shutdown error occurred")
		}
		return fmt.Errorf("errors during shutdown: %v", errs)
	}

	// rs.logger.Info("Service shutdown completed successfully")
	return nil
}

func (rs *RedisService) startBulkProcessor() {
	if rs.cfg.Bulk.Status {

		processor := NewBulkProcessor(rs)
		processor.Start(context.Background())

		// // Use operation manager's shutdown manager to start bulk processor
		// rs.operationManager.GetShutdownManager().StartBulkProcessor(context.Background())
	}
}

// Add bulk operation methods
func (rs *RedisService) AddBulkOperation(ctx context.Context, command string, key string, value interface{}, expires time.Duration) error {
	resultCh := make(chan error, 1)

	select {
	case rs.bulkQueue <- BulkOperation{
		Command:   command,
		Key:       key,
		Value:     value,
		ExpiresAt: expires,
		Result:    resultCh,
	}:
		// Wait for the operation to complete
		select {
		case err := <-resultCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
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
	return rs.wrapper.WrapOperation(ctx, "PING", nil, func() error {
		client := rs.getClient()
		if client == nil {
			return fmt.Errorf("redis client is not initialized")
		}
		return client.Ping(ctx).Err()
	})
}

// GetPoolStats returns current pool statistics with additional safety checks
func (rs *RedisService) GetPoolStats() *redis.PoolStats {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.pool == nil {
		return nil
	}
	return rs.pool.PoolStats()
}

// GetLogger returns the logger instance
func (rs *RedisService) GetLogger() logging.Logger {
	return rs.logger
}
