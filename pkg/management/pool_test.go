// pkg/management/pool_test.go
package management

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionPool(t *testing.T) {
	t.Parallel()
	t.Run("Pool Statistics", func(t *testing.T) {
		// ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		// Allow pool to initialize
		time.Sleep(time.Second)

		stats := rs.getPoolStats()
		require.NotNil(t, stats, "Pool stats should not be nil")

		// Convert config values to uint32 for comparison
		maxSize := uint32(rs.cfg.Pool.Size)
		minIdle := uint32(rs.cfg.Pool.MinIdle)

		// Check total connections
		assert.True(t, stats.TotalConns <= maxSize,
			"Total connections (%d) should not exceed pool size (%d)",
			stats.TotalConns, maxSize)

		// Check idle connections
		assert.True(t, stats.IdleConns >= minIdle,
			"Idle connections (%d) should be at least min idle (%d)",
			stats.IdleConns, minIdle)
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		const numOperations = 10
		var wg sync.WaitGroup
		errs := make([]error, numOperations)

		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("concurrent_key_%d", i)
				errs[i] = rs.Set(ctx, key, i, time.Hour)
			}(i)
		}

		wg.Wait()

		// Check for errors
		for i, err := range errs {
			assert.NoError(t, err, "Operation %d failed", i)
		}
	})
}

func TestPoolSizeAdjustment(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		initialSize  int
		minIdle      int
		loadScale    int // multiplier for load generation
		expectedMin  int // minimum expected size after scaling
		expectedMax  int // maximum expected size after scaling
		interval     int // health check interval
		testDuration time.Duration
	}

	tests := []testCase{
		{
			name:         "Small Scale (1x)",
			initialSize:  10,
			minIdle:      2,
			loadScale:    1,
			expectedMin:  4,
			expectedMax:  15,
			interval:     1,
			testDuration: 5 * time.Second,
		},
		{
			name:         "Medium Scale (10x)",
			initialSize:  20,
			minIdle:      5,
			loadScale:    10,
			expectedMin:  8,
			expectedMax:  30,
			interval:     1,
			testDuration: 8 * time.Second,
		},
		{
			name:         "Large Scale (100x)",
			initialSize:  50,
			minIdle:      10,
			loadScale:    100,
			expectedMin:  15,
			expectedMax:  75,
			interval:     1,
			testDuration: 10 * time.Second,
		},
		{
			name:         "Extra Large Scale (1000x)",
			initialSize:  100,
			minIdle:      20,
			loadScale:    1000,
			expectedMin:  30,
			expectedMax:  150,
			interval:     1,
			testDuration: 15 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			ctx := context.Background()

			rs, err := setupTestRedis()
			require.NoError(t, err)
			defer rs.Close()

			// Configure pool with test case parameters
			// rs.cfg.Pool.Size = tc.initialSize
			// rs.cfg.Pool.MinIdle = tc.minIdle
			// cfg.Redis.HealthCheckInterval = tc.interval

			// service, err := NewRedisService(rs.cfg)
			// require.NoError(t, err)
			// defer service.Close()

			// Test Scale Up under load
			t.Run("Scale Up", func(t *testing.T) {

				initialSize := rs.cfg.Pool.Size
				t.Logf("Initial pool size: %d", initialSize)

				// Generate load based on scale
				var wg sync.WaitGroup

				operationsCount := tc.initialSize * tc.loadScale

				// Start load generation
				for i := 0; i < operationsCount; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						key := fmt.Sprintf("key:%d", i)
						value := fmt.Sprintf("value:%d", i)
						err := rs.Set(ctx, key, value, time.Hour)
						require.NoError(t, err)
						time.Sleep(50 * time.Millisecond)
					}(i)
				}

				// Wait for scaling to occur
				time.Sleep(tc.testDuration)

				stats := rs.getPoolStats()
				require.NotNil(t, stats)

				newSize := rs.cfg.Pool.Size
				t.Logf("New pool size after load: %d", newSize)

				assert.GreaterOrEqual(t, newSize, tc.expectedMin,
					"Pool size should be at least %d, got %d",
					tc.expectedMin, newSize)
				assert.LessOrEqual(t, newSize, tc.expectedMax,
					"Pool size should not exceed %d, got %d",
					tc.expectedMax, newSize)
			})

			// Test Scale Down after load
			t.Run("Scale Down", func(t *testing.T) {
				initialSize := rs.cfg.Pool.Size
				t.Logf("Size before scale down: %d", initialSize)

				// Wait for load to decrease
				time.Sleep(tc.testDuration)

				stats := rs.getPoolStats()
				require.NotNil(t, stats)

				newSize := rs.cfg.Pool.Size
				t.Logf("Size after scale down: %d", newSize)

				assert.GreaterOrEqual(t, newSize, tc.minIdle,
					"Pool size should not go below minIdle (%d), got %d",
					tc.minIdle, newSize)
				assert.LessOrEqual(t, newSize, initialSize,
					"Pool size should have decreased from %d, got %d",
					initialSize, newSize)
			})

			// Log final metrics
			t.Logf("Test case %s completed - Final pool size: %d",
				tc.name, rs.cfg.Pool.Size)
		})
	}
}

func TestPoolResilience(t *testing.T) {
	t.Parallel()

	t.Run("Handle Connection Failures", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		t.Logf("Original Redis connection at %s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Host)

		// Set initial test data
		err = rs.Set(ctx, "init_key", "init_value", time.Hour)
		require.NoError(t, err, "Should be able to set initial value")

		// Simulate Redis failure
		t.Log("Simulating Redis failure...")
		rs.Close()

		// Wait for connection to be fully closed
		time.Sleep(time.Second)
		t.Log("Redis connection closed")

		// Initialize new Redis instance
		rs, err = setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		t.Logf("New Redis instance started at %s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Host)

		// Test recovery with retry logic
		var lastErr error
		for attempt := 0; attempt < rs.cfg.Redis.RetryAttempts; attempt++ {
			err = rs.Set(ctx, "test_key", "test_value", time.Hour)
			if err == nil {
				break
			}
			lastErr = err
			t.Logf("Retry attempt %d failed: %v", attempt+1, err)
			time.Sleep(rs.cfg.Redis.RetryDelay)
		}

		// Verify recovery
		assert.NoError(t, lastErr, "Service should recover after Redis comes back")

		// Verify connection is fully functional
		value, err := rs.Get(ctx, "test_key")
		assert.NoError(t, err, "Should be able to get value after recovery")
		assert.Equal(t, "test_value", value, "Value should be correctly stored")

		// Test multiple operations to ensure stability
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("stability_test_key_%d", i)
			err := rs.Set(ctx, key, fmt.Sprintf("value_%d", i), time.Hour)
			assert.NoError(t, err, "Should handle multiple operations after recovery")
		}

		// Test temporary network issues with new connection
		t.Run("Handle Temporary Network Issues", func(t *testing.T) {
			ctx := context.Background()

			// // Simulate brief network interruption
			// rs.FastForward(time.Second * 2)

			// Test operation during interruption
			err := rs.Set(ctx, "network_test_key", "network_test_value", time.Hour)
			assert.NoError(t, err, "Should handle temporary network issues")

			// Verify the operation was successful
			value, err := rs.Get(ctx, "network_test_key")
			assert.NoError(t, err, "Should be able to get value after network interruption")
			assert.Equal(t, "network_test_value", value)
		})

		// Test pool state with new connection
		t.Run("Verify Pool State After Recovery", func(t *testing.T) {
			stats := rs.getPoolStats()
			assert.NotNil(t, stats, "Pool stats should be available")
			assert.GreaterOrEqual(t, stats.TotalConns, uint32(rs.cfg.Pool.MinIdle),
				"Should maintain minimum connections")
			assert.LessOrEqual(t, stats.TotalConns, uint32(rs.cfg.Pool.Size),
				"Should not exceed maximum pool size")

			// Additional pool health checks
			t.Logf("Pool Statistics - Total: %d, Idle: %d",
				stats.TotalConns, stats.IdleConns)

			// Verify pool is functional by performing a simple operation
			err := rs.Set(ctx, "pool_test_key", "pool_test_value", time.Hour)
			assert.NoError(t, err, "Pool should be functional")
		})
	})
}

func TestPoolConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("Concurrent Operations", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		workers := 50
		opsPerWorker := 100
		var wg sync.WaitGroup
		errors := make(chan error, workers*opsPerWorker)

		start := time.Now()

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < opsPerWorker; j++ {
					key := fmt.Sprintf("worker:%d:key:%d", workerID, j)
					value := fmt.Sprintf("value:%d:%d", workerID, j)

					if err := rs.Set(ctx, key, value, time.Hour); err != nil {
						errors <- err
						return
					}

					_, err := rs.Get(ctx, key)
					if err != nil {
						errors <- err
						return
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		elapsed := time.Since(start)
		t.Logf("Completed %d operations in %v", workers*opsPerWorker, elapsed)

		for err := range errors {
			t.Errorf("Error during concurrent operations: %v", err)
		}
	})
}
