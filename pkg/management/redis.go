package management

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mdastpak/redis-management/config"
	"github.com/mdastpak/redis-management/pkg/logging"
	"github.com/mdastpak/redis-management/pkg/timeout"

	"github.com/go-redis/redis/v8"
)

type RedisService struct {
	// Core components
	cfg            *config.Config
	client         *redis.Client
	keyMgr         *KeyManager
	timeoutManager *timeout.Manager
	logger         logging.Logger

	// Operation handling
	operationManager *OperationManager
	wrapper          *OperationWrapper
	cb               *CircuitBreaker

	// Pool management
	pool        *redis.Client
	poolManager *PoolManager

	// Metrics and synchronization
	metrics *BulkMetrics
	mu      sync.RWMutex
}

func NewRedisService(cfg *config.Config) (*RedisService, error) {
	// Initialize logger
	logger := logging.NewLogger(
		logging.WithLevel(logging.LevelFromConfig(cfg.Logging.Level)),
		logging.WithFormatter(logging.NewFormatter(logging.FormatFromString(cfg.Logging.Format))),
	)

	// Initialize timeout manager
	timeoutManager := timeout.NewManager(&cfg.Timeout)
	if timeoutManager == nil {
		return nil, fmt.Errorf("failed to create timeout manager")
	}

	keyMgr, err := NewKeyManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create key manager: %v", err)
	}

	// Initialize atomic values for metrics
	metrics := &BulkMetrics{}
	metrics.LastOperationTime.Store(time.Now())
	metrics.AverageLatency.Store(time.Duration(0))

	service := &RedisService{
		cfg:            cfg,
		keyMgr:         keyMgr,
		logger:         logger,
		timeoutManager: timeoutManager,
		metrics:        metrics, // Initialize empty metrics
	}

	// Create wrapper after service is initialized
	service.wrapper = NewOperationWrapper(service, logger)

	// Initialize operation manager
	service.operationManager, err = NewOperationManager(service)
	if err != nil {
		return nil, fmt.Errorf("failed to create operation manager: %v", err)
	}

	// Initialize connection
	if err := timeoutManager.ExecuteWithTimeout(context.Background(), "CONNECT", 1,
		func(ctx context.Context) error {
			return service.connect()
		}); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	// Initialize circuit breaker if enabled
	if cfg.Circuit.Status {
		if err := service.initializeCircuitBreaker(); err != nil {
			return nil, fmt.Errorf("failed to initialize circuit breaker: %v", err)
		}
	}

	// Initialize pool if enabled
	if cfg.Pool.Status {
		if err := service.initializePool(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to initialize pool: %v", err)
		}
	}

	service.logger.Info("Redis service initialized successfully")
	return service, nil
}

func (rs *RedisService) initializeCircuitBreaker() error {
	rs.logger.WithFields(map[string]interface{}{
		"threshold":     rs.cfg.Circuit.Threshold,
		"reset_timeout": rs.cfg.Circuit.ResetTimeout,
		"max_half_open": rs.cfg.Circuit.MaxHalfOpen,
	}).Info("Initializing circuit breaker")

	return rs.timeoutManager.ExecuteWithTimeout(context.Background(), "CIRCUIT_INIT", 1,
		func(ctx context.Context) error {
			rs.cb = NewCircuitBreaker(
				rs.cfg.Circuit.Threshold,
				time.Duration(rs.cfg.Circuit.ResetTimeout)*time.Second,
				rs.cfg.Circuit.MaxHalfOpen,
			)

			return nil
		})
}

func (rs *RedisService) initializePool(ctx context.Context) error {

	rs.logger.WithFields(map[string]interface{}{
		"pool_size":    rs.cfg.Pool.Size,
		"min_idle":     rs.cfg.Pool.MinIdle,
		"wait_timeout": rs.cfg.Pool.WaitTimeout,
	}).Info("Initializing connection pool")

	return rs.timeoutManager.ExecuteWithTimeout(ctx, "POOL_INIT", 1,
		func(ctx context.Context) error {
			return rs.NewPoolManager(ctx)
		})
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

// GetTimeoutStats returns timeout manager statistics
func (rs *RedisService) GetTimeoutStats() map[string]timeout.OperationStats {
	return rs.timeoutManager.GetStats()
}

// GetTimeoutStatsAsMap returns timeout manager statistics as a generic map
func (rs *RedisService) GetTimeoutStatsAsMap() map[string]interface{} {
	stats := rs.timeoutManager.GetStats()
	result := make(map[string]interface{}, len(stats))

	for key, stat := range stats {
		result[key] = map[string]interface{}{
			"count":        stat.Count,
			"total_time":   stat.TotalTime.String(),
			"average_time": stat.AverageTime.String(),
			"min_time":     stat.MinTime.String(),
			"max_time":     stat.MaxTime.String(),
			"last_updated": stat.LastUpdated,
		}
	}

	return result
}
