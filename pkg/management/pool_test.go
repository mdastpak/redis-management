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
		// Set appropriate timeout for pool initialization and stats collection
		timeout := time.Duration(10) * time.Second
		if timeout < 5*time.Second {
			timeout = 5 * time.Second
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Initialize test Redis service
		rs, err := setupTestRedis(ctx)
		require.NoError(t, err)
		defer func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			err := rs.Close(closeCtx)
			require.NoError(t, err)
		}()

		// Enable bulk operations for testing
		newCfg := *rs.cfg
		newCfg.Pool.Status = true

		// Apply configuration
		err = rs.ReloadConfig(&newCfg)
		assert.NoError(t, err)

		// Retrieve and verify pool statistics
		stats := rs.GetPoolStats()
		require.NotNil(t, stats, "Pool stats should not be nil")

		// Convert config values for comparison
		maxSize := uint32(rs.cfg.Pool.Size)
		minIdle := uint32(rs.cfg.Pool.MinIdle)

		// Verify connection limits
		assert.True(t, stats.TotalConns <= maxSize,
			"Total connections (%d) should not exceed pool size (%d)",
			stats.TotalConns, maxSize)

		assert.True(t, stats.IdleConns >= minIdle,
			"Idle connections (%d) should be at least min idle (%d)",
			stats.IdleConns, minIdle)
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		// Set timeout for concurrent operations
		timeout := time.Duration(10) * time.Second
		if timeout < 5*time.Second {
			timeout = 5 * time.Second
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Initialize test Redis service
		rs, err := setupTestRedis(ctx)
		require.NoError(t, err)
		defer func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			err := rs.Close(closeCtx)
			require.NoError(t, err)
		}()

		// Define test parameters
		const numOperations = 10

		// Configure Redis service for testing
		newCfg := *rs.cfg
		newCfg.Redis.HashKeys = false
		newCfg.Redis.KeyPrefix = "concurrent_operations:"
		newCfg.Pool.Status = true

		// Apply configuration
		err = rs.ReloadConfig(&newCfg)
		assert.NoError(t, err)

		// Execute concurrent operations
		var wg sync.WaitGroup
		errs := make([]error, numOperations)

		// Launch concurrent operations
		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("key_%d", i)
				errs[i] = rs.Set(ctx, key, i, time.Hour)
			}(i)
		}

		// Wait for all operations to complete
		wg.Wait()

		// Verify operation results
		for i, err := range errs {
			assert.NoError(t, err, "Operation %d failed", i)
		}

		// Verify pool stability
		stats := rs.GetPoolStats()
		require.NotNil(t, stats)
		assert.LessOrEqual(t, stats.TotalConns, uint32(rs.cfg.Pool.Size),
			"Total connections should not exceed pool size")
	})

	t.Run("Pool Health Check", func(t *testing.T) {
		// Set timeout for health checks
		timeout := time.Duration(5) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Initialize test Redis service
		rs, err := setupTestRedis(ctx)
		require.NoError(t, err)
		defer func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			err := rs.Close(closeCtx)
			require.NoError(t, err)
		}()

		// Configure Redis service with pool enabled
		newCfg := *rs.cfg
		newCfg.Pool.Status = true
		newCfg.Pool.Size = 10
		newCfg.Pool.MinIdle = 2

		newCfg.Redis.HashKeys = false
		newCfg.Redis.KeyPrefix = "pool_health_check:"

		// Apply configuration
		err = rs.ReloadConfig(&newCfg)
		assert.NoError(t, err)

		// Verify initial pool state
		initialStats := rs.GetPoolStats()
		require.NotNil(t, initialStats)

		// Perform some operations to trigger pool usage
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key_%d", i)
			err := rs.Set(ctx, key, "value", time.Hour)
			require.NoError(t, err)
		}

		// Allow time for pool to stabilize
		time.Sleep(time.Second)

		// Verify pool health after operations
		finalStats := rs.GetPoolStats()
		require.NotNil(t, finalStats)

		// Verify pool maintains minimum connections
		assert.GreaterOrEqual(t, int(finalStats.IdleConns), newCfg.Pool.MinIdle,
			"Pool should maintain minimum idle connections")

		// Verify pool doesn't exceed maximum size
		assert.LessOrEqual(t, int(finalStats.TotalConns), newCfg.Pool.Size,
			"Pool should not exceed maximum size")
	})
}

func TestPoolSizeAdjustment(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name           string
		initialSize    int
		minIdle        int
		loadScale      int
		expectedMin    int
		expectedMax    int
		poolTimeout    time.Duration
		waitTimeout    time.Duration
		testDuration   time.Duration
		scaleUpTimeout time.Duration
		operationDelay time.Duration
		maxConcurrent  int
	}

	tests := []testCase{
		{
			name:           "Small Scale (1x)",
			initialSize:    10,
			minIdle:        2,
			loadScale:      1,
			expectedMin:    4,
			expectedMax:    15,
			poolTimeout:    5 * time.Second,
			waitTimeout:    10 * time.Second,
			testDuration:   5 * time.Second,
			scaleUpTimeout: 3 * time.Second,
			operationDelay: 100 * time.Millisecond,
			maxConcurrent:  5,
		},
		{
			name:           "Medium Scale (10x)",
			initialSize:    20,
			minIdle:        5,
			loadScale:      10,
			expectedMin:    8,
			expectedMax:    30,
			poolTimeout:    10 * time.Second,
			waitTimeout:    15 * time.Second,
			testDuration:   8 * time.Second,
			scaleUpTimeout: 4 * time.Second,
			operationDelay: 50 * time.Millisecond,
			maxConcurrent:  10,
		},
		{
			name:           "Large Scale (100x)",
			initialSize:    50,
			minIdle:        10,
			loadScale:      100,
			expectedMin:    15,
			expectedMax:    75,
			poolTimeout:    15 * time.Second,
			waitTimeout:    20 * time.Second,
			testDuration:   10 * time.Second,
			scaleUpTimeout: 5 * time.Second,
			operationDelay: 25 * time.Millisecond,
			maxConcurrent:  100,
		},
		{
			name:           "Extra Large Scale (1000x)",
			initialSize:    100,
			minIdle:        20,
			loadScale:      1000,
			expectedMin:    30,
			expectedMax:    150,
			poolTimeout:    20 * time.Second,
			waitTimeout:    25 * time.Second,
			testDuration:   15 * time.Second,
			scaleUpTimeout: 8 * time.Second,
			operationDelay: 10 * time.Millisecond,
			maxConcurrent:  1000,
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create test context
			ctx, cancel := context.WithTimeout(context.Background(), tc.testDuration)
			defer cancel()

			// Initialize Redis service
			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Configure Redis service
			newCfg := *rs.cfg
			newCfg.Redis.HashKeys = false
			newCfg.Redis.KeyPrefix = fmt.Sprintf("test_pool_size_adjustment_%s:", tc.name)

			// Pool configuration
			newCfg.Pool.Status = true
			newCfg.Pool.Size = tc.initialSize
			newCfg.Pool.MinIdle = tc.minIdle
			newCfg.Pool.MaxIdleTime = tc.minIdle * 10
			newCfg.Pool.WaitTimeout = int(tc.waitTimeout.Seconds())

			// Retry and timeout settings
			newCfg.Redis.RetryAttempts = 3
			newCfg.Redis.RetryDelay = time.Second
			newCfg.Redis.MaxRetryBackoff = 5 * time.Second
			newCfg.Redis.Timeout = int(tc.poolTimeout.Seconds())

			err = rs.ReloadConfig(&newCfg)
			assert.NoError(t, err)

			// Wait for pool to initialize
			time.Sleep(time.Second)

			// Test Scale Up
			// Test Scale Up
			t.Run("Scale Up", func(t *testing.T) {
				initialMetrics := rs.poolManager.getCurrentMetrics()
				require.NotNil(t, initialMetrics)
				t.Logf("Initial metrics - Total: %d, Active: %d, Idle: %d",
					initialMetrics.TotalConnections,
					initialMetrics.ActiveConnections,
					initialMetrics.IdleConnections)

				operationsCount := tc.initialSize * tc.loadScale
				errorChan := make(chan error, operationsCount)
				completedOps := make(chan struct{}, operationsCount)

				// Use semaphore to limit concurrent operations
				sem := make(chan struct{}, tc.maxConcurrent)

				var wg sync.WaitGroup
				for i := 0; i < operationsCount; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()

						// Acquire semaphore
						sem <- struct{}{}
						defer func() { <-sem }()

						key := fmt.Sprintf("key_%d", i)
						value := fmt.Sprintf("value_%d", i)

						if err := rs.Set(ctx, key, value, time.Hour); err != nil {
							select {
							case errorChan <- fmt.Errorf("operation %d failed: %v", i, err):
							default:
							}
							return
						}

						// Add delay between operations
						time.Sleep(tc.operationDelay)

						completedOps <- struct{}{}
					}(i)
				}

				// Monitor scaling in separate goroutine
				done := make(chan struct{})
				var finalMetrics *PoolMetrics

				go func() {
					defer close(done)
					ticker := time.NewTicker(100 * time.Millisecond)
					defer ticker.Stop()

					for {
						select {
						case <-ctx.Done():
							return
						case err := <-errorChan:
							t.Logf("Operation error: %v", err)
						case <-ticker.C:
							metrics := rs.poolManager.getCurrentMetrics()
							if metrics != nil {
								finalMetrics = metrics
								if int(metrics.TotalConnections) >= tc.expectedMin {
									return
								}
							}
						}
					}
				}()

				// Wait for completion or timeout
				select {
				case <-done:
					// Success case
				case <-time.After(tc.scaleUpTimeout):
					t.Log("Scale up timeout reached")
				}

				// Log final metrics
				if finalMetrics != nil {
					t.Logf("Final metrics - Total: %d, Active: %d, Idle: %d",
						finalMetrics.TotalConnections,
						finalMetrics.ActiveConnections,
						finalMetrics.IdleConnections)

					assert.GreaterOrEqual(t, int(finalMetrics.TotalConnections), tc.expectedMin,
						"Pool size should be at least %d", tc.expectedMin)
					assert.LessOrEqual(t, int(finalMetrics.TotalConnections), tc.expectedMax,
						"Pool size should not exceed %d", tc.expectedMax)
				}

				// Wait for outstanding operations
				wg.Wait()
			})

			// Test Scale Down
			t.Run("Scale Down", func(t *testing.T) {
				initialMetrics := rs.poolManager.getCurrentMetrics()
				require.NotNil(t, initialMetrics)
				t.Logf("Initial scale down metrics - Total: %d, Active: %d, Idle: %d",
					initialMetrics.TotalConnections,
					initialMetrics.ActiveConnections,
					initialMetrics.IdleConnections)

				// Wait for scale down
				time.Sleep(tc.operationDelay)

				finalMetrics := rs.poolManager.getCurrentMetrics()
				require.NotNil(t, finalMetrics)
				t.Logf("Final scale down metrics - Total: %d, Active: %d, Idle: %d",
					finalMetrics.TotalConnections,
					finalMetrics.ActiveConnections,
					finalMetrics.IdleConnections)

				assert.GreaterOrEqual(t, int(finalMetrics.TotalConnections), tc.minIdle,
					"Pool size should not go below minIdle (%d), got %d",
					tc.minIdle, finalMetrics.TotalConnections)
				assert.LessOrEqual(t, int(finalMetrics.TotalConnections), int(initialMetrics.TotalConnections),
					"Pool size should have decreased from %d, got %d",
					initialMetrics.TotalConnections, finalMetrics.TotalConnections)
			})

			// Log final state
			finalMetrics := rs.poolManager.getCurrentMetrics()
			t.Logf("Test case %s completed - Final metrics - Total: %d, Active: %d, Idle: %d",
				tc.name,
				finalMetrics.TotalConnections,
				finalMetrics.ActiveConnections,
				finalMetrics.IdleConnections)
		})
	}
}

func TestPoolResilience(t *testing.T) {
	t.Parallel()

	t.Run("Handle Connection Failures", func(t *testing.T) {
		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Setup initial Redis instance
		rs, err := setupTestRedis(ctx)
		require.NoError(t, err)

		// Store initial configuration for reuse
		initialConfig := *rs.cfg

		// Enable pool and configure with proper retry settings
		newCfg := initialConfig
		newCfg.Pool.Status = true
		newCfg.Pool.Size = 10
		newCfg.Pool.MinIdle = 2
		newCfg.Redis.RetryAttempts = 3
		newCfg.Redis.RetryDelay = time.Second
		newCfg.Redis.MaxRetryBackoff = 5 * time.Second
		newCfg.Redis.HealthCheckInterval = 1

		err = rs.ReloadConfig(&newCfg)
		require.NoError(t, err)

		t.Logf("Initial Redis instance at %s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Port)

		// Set initial test data
		err = rs.Set(ctx, "init_key", "init_value", time.Hour)
		require.NoError(t, err, "Should set initial value")

		// Store initial metrics for comparison
		require.NotNil(t, rs.poolManager, "Pool manager should be initialized")
		initialMetrics := rs.poolManager.getCurrentMetrics()
		require.NotNil(t, initialMetrics)
		t.Logf("Initial metrics - Total: %d, Active: %d, Idle: %d",
			initialMetrics.TotalConnections,
			initialMetrics.ActiveConnections,
			initialMetrics.IdleConnections)

		// Simulate failure by closing connection
		t.Log("Simulating Redis failure...")
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = rs.Close(closeCtx)
		closeCancel()
		require.NoError(t, err)

		// Allow time for connections to close
		time.Sleep(2 * time.Second)
		t.Log("Redis connection closed")

		// Initialize new instance
		rs, err = setupTestRedis(ctx)
		require.NoError(t, err)
		defer func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			err := rs.Close(closeCtx)
			require.NoError(t, err)
		}()

		// Reapply pool configuration to new instance
		t.Log("Reconfiguring new instance with pool settings...")
		err = rs.ReloadConfig(&newCfg)
		require.NoError(t, err, "Should reconfigure pool settings")

		// Wait for pool to initialize
		time.Sleep(2 * time.Second)

		// Test recovery with exponential backoff
		t.Log("Testing recovery...")
		var lastErr error
		backoff := rs.cfg.Redis.RetryDelay
		maxBackoff := rs.cfg.Redis.MaxRetryBackoff

		for attempt := 1; attempt <= rs.cfg.Redis.RetryAttempts; attempt++ {
			err = rs.Set(ctx, "test_key", "test_value", time.Hour)
			if err == nil {
				t.Logf("Recovery successful on attempt %d", attempt)
				break
			}
			lastErr = err
			t.Logf("Recovery attempt %d failed: %v", attempt, err)

			if attempt < rs.cfg.Redis.RetryAttempts {
				sleepTime := backoff
				if sleepTime > maxBackoff {
					sleepTime = maxBackoff
				}
				t.Logf("Waiting %v before next attempt", sleepTime)
				time.Sleep(sleepTime)
				backoff *= 2
			}
		}

		assert.NoError(t, lastErr, "Service should recover after reconnection")

		// Verify connection functionality
		t.Run("Verify Connection Recovery", func(t *testing.T) {
			// Test basic operations
			value, err := rs.Get(ctx, "test_key")
			assert.NoError(t, err, "Should read after recovery")
			assert.Equal(t, "test_value", value)

			// Test multiple operations
			var wg sync.WaitGroup
			errors := make(chan error, 5)

			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := fmt.Sprintf("recovery_test_key_%d", i)
					value := fmt.Sprintf("value_%d", i)
					if err := rs.Set(ctx, key, value, time.Hour); err != nil {
						errors <- fmt.Errorf("operation %d failed: %v", i, err)
					}
				}(i)
			}

			wg.Wait()
			close(errors)

			for err := range errors {
				t.Errorf("Post-recovery operation error: %v", err)
			}
		})

		// Verify pool metrics after recovery
		t.Run("Verify Pool Metrics After Recovery", func(t *testing.T) {
			require.NotNil(t, rs.poolManager, "Pool manager should be initialized after recovery")
			metrics := rs.poolManager.getCurrentMetrics()
			require.NotNil(t, metrics, "Pool metrics should be available")

			t.Logf("Post-recovery metrics - Total: %d, Active: %d, Idle: %d",
				metrics.TotalConnections,
				metrics.ActiveConnections,
				metrics.IdleConnections)

			assert.GreaterOrEqual(t, metrics.TotalConnections, int64(rs.cfg.Pool.MinIdle),
				"Should maintain minimum connections")
			assert.LessOrEqual(t, metrics.TotalConnections, int64(rs.cfg.Pool.Size),
				"Should not exceed maximum size")
		})
	})
}

func TestPoolConcurrency(t *testing.T) {
	t.Parallel()

	type testParams struct {
		workers       int
		opsPerWorker  int
		maxConcurrent int
		timeout       time.Duration
	}

	tests := []struct {
		name   string
		params testParams
	}{
		{
			name: "Light Load",
			params: testParams{
				workers:       10,
				opsPerWorker:  50,
				maxConcurrent: 5,
				timeout:       10 * time.Second,
			},
		},
		{
			name: "Heavy Load",
			params: testParams{
				workers:       50,
				opsPerWorker:  100,
				maxConcurrent: 20,
				timeout:       30 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), tt.params.timeout)
			defer cancel()

			// Initialize Redis service
			rs, err := setupTestRedis(ctx)
			require.NoError(t, err)
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				err := rs.Close(closeCtx)
				require.NoError(t, err)
			}()

			// Configure connection pool
			newCfg := *rs.cfg
			newCfg.Pool.Status = true
			newCfg.Pool.Size = tt.params.maxConcurrent * 2
			newCfg.Pool.MinIdle = tt.params.maxConcurrent / 2
			err = rs.ReloadConfig(&newCfg)
			require.NoError(t, err)

			// Create channels for results and errors
			results := make(chan struct{}, tt.params.workers*tt.params.opsPerWorker)
			errors := make(chan error, tt.params.workers*tt.params.opsPerWorker)

			// Create semaphore for concurrency control
			sem := make(chan struct{}, tt.params.maxConcurrent)

			var wg sync.WaitGroup
			start := time.Now()

			// Start workers
			for i := 0; i < tt.params.workers; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for j := 0; j < tt.params.opsPerWorker; j++ {
						select {
						case <-ctx.Done():
							return
						case sem <- struct{}{}: // Acquire semaphore
							func() {
								defer func() { <-sem }() // Release semaphore
								key := fmt.Sprintf("worker:%d:key:%d", workerID, j)
								value := fmt.Sprintf("value:%d:%d", workerID, j)

								// Set value
								if err := rs.Set(ctx, key, value, time.Hour); err != nil {
									errors <- fmt.Errorf("set error (worker %d, op %d): %v",
										workerID, j, err)
									return
								}

								// Verify value
								readValue, err := rs.Get(ctx, key)
								if err != nil {
									errors <- fmt.Errorf("get error (worker %d, op %d): %v",
										workerID, j, err)
									return
								}
								if readValue != value {
									errors <- fmt.Errorf("value mismatch (worker %d, op %d)",
										workerID, j)
									return
								}

								results <- struct{}{}
							}()
						}
					}
				}(i)
			}

			// Wait for completion
			go func() {
				wg.Wait()
				close(results)
				close(errors)
			}()

			// Collect results
			totalOps := 0
			for range results {
				totalOps++
			}

			elapsed := time.Since(start)
			opsPerSec := float64(totalOps) / elapsed.Seconds()

			// Log performance metrics
			t.Logf("Performance Metrics for %s:", tt.name)
			t.Logf("- Total Operations: %d", totalOps)
			t.Logf("- Time Elapsed: %v", elapsed)
			t.Logf("- Operations/second: %.2f", opsPerSec)

			// Check for errors
			var errCount int
			for err := range errors {
				errCount++
				t.Errorf("Operation error: %v", err)
			}

			// Verify final metrics
			metrics := rs.poolManager.getCurrentMetrics()
			t.Logf("Final Pool Metrics:")
			t.Logf("- Total Connections: %d", metrics.TotalConnections)
			t.Logf("- Active Connections: %d", metrics.ActiveConnections)
			t.Logf("- Idle Connections: %d", metrics.IdleConnections)
			t.Logf("- Error Rate: %.2f%%", float64(errCount)/float64(totalOps)*100)

			assert.Equal(t, tt.params.workers*tt.params.opsPerWorker, totalOps+errCount,
				"Total operations should match expected count")
		})
	}
}
