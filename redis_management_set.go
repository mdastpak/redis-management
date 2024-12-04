package redismanagement

import (
	"context"
	"fmt"
	"time"
)

// Set stores a key-value pair in Redis with specified TTL (0 means no expiration)
func (c *Client) Set(key string, value interface{}, ttl time.Duration) error {
	ctx := context.Background()
	return c.service.Set(ctx, key, value, ttl)
}

// SetWithDefaultTTL stores a key-value pair using configured default TTL
func (c *Client) SetWithDefaultTTL(key string, value interface{}) error {
	ctx := context.Background()
	return c.service.SetWithDefaultTTL(ctx, key, value)
}

// SetBatch stores multiple key-value pairs with specified TTL
func (c *Client) SetBatch(items map[string]interface{}, ttl time.Duration) error {
	ctx := context.Background()
	return c.service.SetBatch(ctx, items, ttl)
}

// SetBatchWithDefaultTTL stores multiple key-value pairs using configured default TTL
func (c *Client) SetBatchWithDefaultTTL(items map[string]interface{}) error {
	ctx := context.Background()
	return c.service.SetBatchWithDefaultTTL(ctx, items)
}

// BulkSet performs multiple Set operations with specified TTL
func (c *Client) SetBulk(items map[string]interface{}, ttl time.Duration) error {
	ctx := context.Background()
	return c.SetBulkWithContext(ctx, items, ttl)
}

// BulkSetWithDefaultTTL performs multiple Set operations using configured default TTL
func (c *Client) SetBulkWithDefaultTTL(items map[string]interface{}) error {
	ctx := context.Background()
	return c.SetBulkWithContext(ctx, items, c.cfg.Redis.TTL)
}

// BulkSetWithContext performs multiple Set operations with context
func (c *Client) SetBulkWithContext(ctx context.Context, items map[string]interface{}, ttl time.Duration) error {
	for key, value := range items {
		if err := c.service.AddBulkOperation(ctx, "SET", key, value, ttl); err != nil {
			return fmt.Errorf("failed to add bulk operation for key %s: %v", key, err)
		}
	}
	return nil
}
