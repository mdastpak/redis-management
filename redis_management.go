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

// NewClient creates a new Redis management client with the provided configuration file
func NewClient(configPath string) (*Client, error) {
	// Load configuration
	cfg, err := config.Load(configPath)
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

// NewClientWithConfig creates a new Redis management client with provided config struct
func NewClientWithConfig(cfg *config.Config) (*Client, error) {
	service, err := management.NewRedisService(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %v", err)
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

// Set stores a key-value pair in Redis
func (c *Client) Set(key string, value interface{}) error {
	return c.SetWithTTL(key, value, 0)
}

// SetWithTTL stores a key-value pair with an expiration time
func (c *Client) SetWithTTL(key string, value interface{}, ttl time.Duration) error {
	ctx := context.Background()
	return c.SetWithContext(ctx, key, value, ttl)
}

// SetWithContext stores a key-value pair with context
func (c *Client) SetWithContext(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return c.service.Set(ctx, key, value, ttl)
}

// Get retrieves a value by key
func (c *Client) Get(key string) (string, error) {
	ctx := context.Background()
	return c.GetWithContext(ctx, key)
}

// GetWithContext retrieves a value by key with context
func (c *Client) GetWithContext(ctx context.Context, key string) (string, error) {
	return c.service.Get(ctx, key)
}

// Delete removes a key from Redis
func (c *Client) Delete(key string) error {
	ctx := context.Background()
	return c.DeleteWithContext(ctx, key)
}

// DeleteWithContext removes a key with context
func (c *Client) DeleteWithContext(ctx context.Context, key string) error {
	return c.service.Delete(ctx, key)
}

// SetBatch stores multiple key-value pairs
func (c *Client) SetBatch(items map[string]interface{}) error {
	return c.SetBatchWithTTL(items, 0)
}

// SetBatchWithTTL stores multiple key-value pairs with expiration
func (c *Client) SetBatchWithTTL(items map[string]interface{}, ttl time.Duration) error {
	ctx := context.Background()
	return c.SetBatchWithContext(ctx, items, ttl)
}

// SetBatchWithContext stores multiple key-value pairs with context
func (c *Client) SetBatchWithContext(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	return c.service.SetBatch(ctx, items, ttl)
}

// GetBatch retrieves multiple values
func (c *Client) GetBatch(keys []string) (map[string]string, error) {
	ctx := context.Background()
	return c.GetBatchWithContext(ctx, keys)
}

// GetBatchWithContext retrieves multiple values with context
func (c *Client) GetBatchWithContext(ctx context.Context, keys []string) (map[string]string, error) {
	return c.service.GetBatch(ctx, keys)
}

// DeleteBatch removes multiple keys
func (c *Client) DeleteBatch(keys []string) error {
	ctx := context.Background()
	return c.DeleteBatchWithContext(ctx, keys)
}

// DeleteBatchWithContext removes multiple keys with context
func (c *Client) DeleteBatchWithContext(ctx context.Context, keys []string) error {
	return c.service.DeleteBatch(ctx, keys)
}

// BulkSet performs multiple Set operations efficiently
func (c *Client) BulkSet(items map[string]interface{}) error {
	return c.BulkSetWithTTL(items, 0)
}

// BulkSetWithTTL performs multiple Set operations with expiration
func (c *Client) BulkSetWithTTL(items map[string]interface{}, ttl time.Duration) error {
	ctx := context.Background()
	return c.BulkSetWithContext(ctx, items, ttl)
}

// BulkSetWithContext performs multiple Set operations with context
func (c *Client) BulkSetWithContext(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	for key, value := range items {
		if err := c.service.AddBulkOperation(ctx, "SET", key, value, ttl); err != nil {
			return fmt.Errorf("failed to add bulk operation for key %s: %v", key, err)
		}
	}
	return nil
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
