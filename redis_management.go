// redis_management.go
package redismanagement

import (
	"context"
	"fmt"
	"time"

	"github.com/mdastpak/redis-management/config"
	"github.com/mdastpak/redis-management/pkg/management"
)

// Client represents the main Redis management client
type Client struct {
	service *management.RedisService
	cfg     *config.Config
}

// NewClient creates a new Redis management client with default configuration
func NewClient() (*Client, error) {
	// Load configuration
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}
	// Create service
	service, err := management.NewRedisService(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %v", err)
	}
	return &Client{
		service: service,
		cfg:     cfg,
	}, nil
}

// NewClientWithCustomConfig creates a new Redis management client with custom configuration
func NewClientWithCustomConfig(customConfig map[string]interface{}) (*Client, error) {
	// Get default config
	defaultClient, err := NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create default client: %v", err)
	}

	cfg := defaultClient.cfg

	// Apply custom configuration
	for key, value := range customConfig {
		switch key {
		// Redis configuration
		case "redis_host":
			cfg.Redis.Host = value.(string)
		case "redis_port":
			cfg.Redis.Port = value.(string)
		case "redis_password":
			cfg.Redis.Password = value.(string)
		case "redis_db":
			cfg.Redis.DB = value.(string)
		case "redis_key_prefix":
			cfg.Redis.KeyPrefix = value.(string)
		case "redis_ttl":
			cfg.Redis.TTL = value.(time.Duration)
		case "redis_timeout":
			cfg.Redis.Timeout = value.(int)
		case "redis_hash_keys":
			cfg.Redis.HashKeys = value.(bool)
		case "redis_health_check_interval":
			cfg.Redis.HealthCheckInterval = value.(int)
		case "redis_retry_attempts":
			cfg.Redis.RetryAttempts = value.(int)
		case "redis_retry_delay":
			cfg.Redis.RetryDelay = value.(time.Duration)
		case "redis_max_retry_backoff":
			cfg.Redis.MaxRetryBackoff = value.(time.Duration)
		case "redis_shutdown_timeout":
			cfg.Redis.ShutdownTimeout = value.(time.Duration)

		// Pool configuration
		case "pool_status":
			cfg.Pool.Status = value.(bool)
		case "pool_size":
			cfg.Pool.Size = value.(int)
		case "pool_min_idle":
			cfg.Pool.MinIdle = value.(int)
		case "pool_max_idle_time":
			cfg.Pool.MaxIdleTime = value.(int)
		case "pool_wait_timeout":
			cfg.Pool.WaitTimeout = value.(int)

		// Bulk configuration
		case "bulk_status":
			cfg.Bulk.Status = value.(bool)
		case "bulk_batch_size":
			cfg.Bulk.BatchSize = value.(int)
		case "bulk_flush_interval":
			cfg.Bulk.FlushInterval = value.(int)
		case "bulk_max_retries":
			cfg.Bulk.MaxRetries = value.(int)
		case "bulk_concurrent_flush":
			cfg.Bulk.ConcurrentFlush = value.(bool)

		// Logging configuration
		case "logging_level":
			cfg.Logging.Level = value.(string)
		case "logging_format":
			cfg.Logging.Format = value.(string)
		case "logging_output":
			cfg.Logging.Output = value.(string)
		case "logging_file_path":
			cfg.Logging.FilePath = value.(string)

		// Circuit breaker configuration
		case "circuit_status":
			cfg.Circuit.Status = value.(bool)
		case "circuit_threshold":
			cfg.Circuit.Threshold = value.(int64)
		case "circuit_reset_timeout":
			cfg.Circuit.ResetTimeout = value.(int)
		case "circuit_max_half_open":
			cfg.Circuit.MaxHalfOpen = value.(int32)
		}
	}

	// Create new service with modified config
	service, err := management.NewRedisService(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create service with custom config: %v", err)
	}

	return &Client{
		service: service,
		cfg:     cfg,
	}, nil
}

// Close closes the client and its connections
func (c *Client) Close() error {
	return c.service.Close()
}

// GetConfig returns the current configuration
func (c *Client) GetConfig() *config.Config {
	return c.cfg
}

// GetStats returns the current pool and service statistics
func (c *Client) GetStats() interface{} {
	return c.service.GetPoolStats()
}

// HealthCheck performs a health check on the Redis connection
func (c *Client) HealthCheck() error {
	ctx := context.Background()
	ping := c.service.Ping(ctx)
	if ping != nil {
		return fmt.Errorf("health check failed: %v", ping)
	}
	return nil
}
