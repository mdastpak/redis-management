package redismanagement

import (
	"context"
	"time"
)

// TTL operations
func (c *Client) GetTTL(key string) (time.Duration, error) {
	return c.GetTTLWithContext(context.Background(), key)
}

func (c *Client) GetTTLWithContext(ctx context.Context, key string) (time.Duration, error) {
	return c.service.GetTTL(ctx, key)
}

func (c *Client) SetTTL(key string, ttl time.Duration) error {
	return c.SetTTLWithContext(context.Background(), key, ttl)
}

func (c *Client) SetTTLWithContext(ctx context.Context, key string, ttl time.Duration) error {
	return c.service.SetTTL(ctx, key, ttl)
}

// Batch TTL operations
func (c *Client) GetBatchTTL(keys []string) (map[string]time.Duration, error) {
	return c.GetBatchTTLWithContext(context.Background(), keys)
}

func (c *Client) GetBatchTTLWithContext(ctx context.Context, keys []string) (map[string]time.Duration, error) {
	return c.service.GetBatchTTL(ctx, keys)
}

func (c *Client) SetBatchTTL(keys []string, ttl time.Duration) error {
	return c.SetBatchTTLWithContext(context.Background(), keys, ttl)
}

func (c *Client) SetBatchTTLWithContext(ctx context.Context, keys []string, ttl time.Duration) error {
	return c.service.SetBatchTTL(ctx, keys, ttl)
}
