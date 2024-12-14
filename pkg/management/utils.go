package management

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function for checking TTL
func assertTTL(t *testing.T, service *RedisService, key string, expectedTTL time.Duration) {
	ttl, err := service.GetTTL(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, expectedTTL, ttl)
}

// Helper function for checking key value
func assertKeyValue(t *testing.T, service *RedisService, key string, expectedValue interface{}) {
	value, err := service.Get(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, expectedValue, value)
}

// Helper function for choosing minimum value
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
