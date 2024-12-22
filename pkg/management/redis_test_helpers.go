// pkg/management/redis_test_helpers.go

package management

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"redis-management/config"

	"github.com/stretchr/testify/require"
)

type testSetupConfig struct {
	baseTimeout  time.Duration
	cleanup      bool
	scale        int                  // for scaling timeouts based on operation complexity
	modifyConfig func(*config.Config) // Add config modifier function
}

type testSetupOption func(*testSetupConfig)

// WithTimeout allows custom timeout for specific tests
func WithTimeout(d time.Duration) testSetupOption {
	return func(c *testSetupConfig) {
		c.baseTimeout = d
	}
}

// WithScale allows scaling the timeout based on operation complexity
func WithScale(scale int) testSetupOption {
	return func(c *testSetupConfig) {
		c.scale = scale
	}
}

// WithoutCleanup disables automatic cleanup
func WithoutCleanup() testSetupOption {
	return func(c *testSetupConfig) {
		c.cleanup = false
	}
}

// WithInitialConfig allows modifying the config before service creation
func WithInitialConfig(modifier func(*config.Config)) testSetupOption {
	return func(c *testSetupConfig) {
		c.modifyConfig = modifier
	}
}

// setupTestRedisWithConfig creates a test Redis service with configurable timeout and initial config
func setupTestRedisWithConfig(t *testing.T, opts ...testSetupOption) (*RedisService, context.Context, context.CancelFunc) {
	cfg := &testSetupConfig{
		baseTimeout: 30 * time.Second,
		cleanup:     true,
		scale:       1,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	timeout := cfg.baseTimeout * time.Duration(cfg.scale)
	t.Logf("Setting up test with timeout: %v (baseTimeout: %v, scale: %d)",
		timeout, cfg.baseTimeout, cfg.scale)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	// Get default config
	redisConfig, err := config.Load()
	require.NoError(t, err)

	// Apply custom configuration if provided
	if cfg.modifyConfig != nil {
		cfg.modifyConfig(redisConfig)
		// Validate modified config
		err = config.ValidateConfig(redisConfig)
		require.NoError(t, err, "Invalid configuration after modifications")

		// t.Logf("Applied custom configuration: %+v", redisConfig)
	}

	// Create service with modified config
	rs, err := NewRedisService(redisConfig)
	require.NoError(t, err)

	if !redisConfig.Pool.Status && cfg.cleanup {
		t.Cleanup(func() {
			// fmt.Println("setupTestRedisWithConfig - Cleaning up test resources")
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			closeErr := rs.Close(cleanupCtx)
			// Check close error only when we don't expect an error
			// For tests that expect shutdown errors, we shouldn't fail here
			if !strings.Contains(t.Name(), "Timeout") && !strings.Contains(t.Name(), "Error") {
				require.NoError(t, closeErr)
			}
		})
	}

	return rs, ctx, cancel
}

// Helper function for operations that need retries
func withRetries(t *testing.T, attempts int, operation func() error) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := operation(); err != nil {
			lastErr = err
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			continue
		}
		return nil
	}
	return lastErr
}

// Helper function to be used across all test files
func setupTestRedis(ctx context.Context) (*RedisService, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

	rs, err := NewRedisService(cfg)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Service initialized with wrapper: %v\n", rs.wrapper != nil)

	return rs, nil
}
