package redismanagement

import "context"

// Get retrieves a value by key
func (c *Client) Get(key string) (string, error) {
	ctx := context.Background()
	return c.GetWithContext(ctx, key)
}

// GetWithContext retrieves a value by key with context
func (c *Client) GetWithContext(ctx context.Context, key string) (string, error) {
	return c.service.Get(ctx, key)
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
