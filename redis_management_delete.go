package redismanagement

import "context"

// Delete removes a key from Redis
func (c *Client) Delete(key string) error {
	ctx := context.Background()
	return c.DeleteWithContext(ctx, key)
}

// DeleteWithContext removes a key with context
func (c *Client) DeleteWithContext(ctx context.Context, key string) error {
	return c.service.Delete(ctx, key)
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
