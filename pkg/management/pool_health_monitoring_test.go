package management

import (
	"context"
	"fmt"
	"math/rand"
	"redis-management/config"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolHealthMonitoring(t *testing.T) {
	t.Parallel()

	t.Run("Connection Health Checks", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t,
			WithInitialConfig(func(cfg *config.Config) {
				cfg.Pool.Status = true
				cfg.Pool.Size = 10
				cfg.Pool.MinIdle = 2
				cfg.Redis.HealthCheckInterval = 1
			}),
		)
		defer cancel()

		// Monitor health check metrics
		var healthChecks int32
		var failedChecks int32
		monitorDuration := 5 * time.Second

		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			start := time.Now()
			for time.Since(start) < monitorDuration {
				select {
				case <-ticker.C:
					metrics := rs.poolManager.getCurrentMetrics()
					if metrics != nil {
						atomic.AddInt32(&healthChecks, 1)
						if metrics.ErrorCount > 0 {
							atomic.AddInt32(&failedChecks, 1)
						}
					}
				case <-ctx.Done():
					return
				}
			}
			close(done)
		}()

		<-done

		assert.Greater(t, atomic.LoadInt32(&healthChecks), int32(0),
			"Should perform health checks")
		assert.Equal(t, int32(0), atomic.LoadInt32(&failedChecks),
			"Should have no failed health checks")
	})

	t.Run("Connection Cleanup", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t,
			WithInitialConfig(func(cfg *config.Config) {
				cfg.Pool.Status = true
				cfg.Pool.Size = 20
				cfg.Pool.MinIdle = 5
				cfg.Pool.MaxIdleTime = 1 // Short idle time for testing
			}),
		)
		defer cancel()

		// Generate initial load to create connections
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				key := fmt.Sprintf("cleanup_test_key_%d", id)
				value := fmt.Sprintf("value_%d", id)
				err := rs.Set(ctx, key, value, time.Hour)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Wait for cleanup cycle
		time.Sleep(2 * time.Second)

		// Verify connection cleanup
		metrics := rs.poolManager.getCurrentMetrics()
		require.NotNil(t, metrics)

		assert.LessOrEqual(t, metrics.IdleConnections, int64(rs.cfg.Pool.Size),
			"Should not exceed max pool size")
		assert.GreaterOrEqual(t, metrics.IdleConnections, int64(rs.cfg.Pool.MinIdle),
			"Should maintain minimum idle connections")

		// Track cleanup over time
		cleanupChecks := 5
		for i := 0; i < cleanupChecks; i++ {
			time.Sleep(500 * time.Millisecond)
			currentMetrics := rs.poolManager.getCurrentMetrics()
			require.NotNil(t, currentMetrics)
			assert.GreaterOrEqual(t, currentMetrics.IdleConnections, int64(rs.cfg.Pool.MinIdle),
				"Should never go below minimum idle connections")
		}
	})

	t.Run("Connection Recovery", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t,
			WithInitialConfig(func(cfg *config.Config) {
				// Pool configuration
				cfg.Pool.Status = true
				cfg.Pool.Size = 30
				cfg.Pool.MinIdle = 5
				cfg.Pool.WaitTimeout = int(cfg.Timeout.Operations.BulkBase.Seconds())
				cfg.Pool.MaxIdleTime = int(cfg.Timeout.MaxTimeout.Seconds())

				// Redis retry configuration
				cfg.Redis.RetryAttempts = 3
				cfg.Redis.RetryDelay = cfg.Timeout.MinTimeout
				cfg.Redis.MaxRetryBackoff = cfg.Timeout.MaxTimeout
				cfg.Redis.ShutdownTimeout = cfg.Timeout.MaxTimeout

				// Timeout configuration
				cfg.Timeout = config.TimeoutConfig{
					BaseTimeout:   5 * time.Second,
					MaxTimeout:    30 * time.Second,
					MinTimeout:    time.Second,
					BackoffFactor: 1.5,
					Operations: config.OperationTimeoutConfig{
						Get:      time.Second,
						Set:      2 * time.Second,
						Delete:   2 * time.Second,
						BulkBase: 5 * time.Second,
					},
					Adaptive: config.AdaptiveTimeoutConfig{
						Enabled:             true,
						WindowSize:          100,
						AdjustmentThreshold: 0.2,
						MaxAdjustment:       0.5,
						HistoryRetention:    24 * time.Hour,
					},
				}
			}),
		)
		defer cancel()

		failureScenarios := []struct {
			name         string
			operations   int
			expectedErrs int
			recoveryTime time.Duration
			concurrent   int
		}{
			{
				name:         "Timeout Recovery",
				operations:   20,
				expectedErrs: 5,
				recoveryTime: rs.cfg.Timeout.Operations.BulkBase,
				concurrent:   10,
			},
			{
				name:         "Connection Drop Recovery",
				operations:   30,
				expectedErrs: 8,
				recoveryTime: rs.cfg.Timeout.Operations.BulkBase * 2,
				concurrent:   15,
			},
			{
				name:         "Partial Failure Recovery",
				operations:   25,
				expectedErrs: 6,
				recoveryTime: rs.cfg.Timeout.Operations.BulkBase,
				concurrent:   12,
			},
		}

		for _, scenario := range failureScenarios {
			t.Run(scenario.name, func(t *testing.T) {
				// Create operation context with timeout
				opCtx, opCancel := context.WithTimeout(ctx, rs.cfg.Timeout.Operations.BulkBase)
				defer opCancel()

				sem := make(chan struct{}, scenario.concurrent)
				errors := make(chan error, scenario.operations)
				var wg sync.WaitGroup

				for i := 0; i < scenario.operations; i++ {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						sem <- struct{}{}
						defer func() { <-sem }()

						// Use timeout manager for operation
						err := rs.timeoutManager.ExecuteWithTimeout(opCtx, "SET", 1,
							func(ctx context.Context) error {
								key := fmt.Sprintf("recovery_test_key_%d", id)
								return rs.Set(ctx, key, "test_value", time.Hour)
							})

						if err != nil {
							select {
							case errors <- err:
							default:
							}
						}
					}(i)
				}

				// Wait with timeout
				waitChan := make(chan struct{})
				go func() {
					wg.Wait()
					close(waitChan)
				}()

				select {
				case <-waitChan:
					t.Log("Operations completed")
				case <-time.After(rs.cfg.Timeout.MaxTimeout):
					t.Log("Operations timed out")
				}

				close(errors)

				var errCount int
				for range errors {
					errCount++
				}

				t.Logf("%s - Errors: %d/%d (expected/actual)",
					scenario.name, scenario.expectedErrs, errCount)

				// Allow for recovery using configured timeout
				time.Sleep(scenario.recoveryTime)

				// Verify pool status with timeout
				verifyCtx, verifyCancel := context.WithTimeout(ctx, rs.cfg.Timeout.Operations.Set)
				defer verifyCancel()

				metrics := rs.poolManager.getCurrentMetrics()
				require.NotNil(t, metrics)

				t.Logf("%s - Final Metrics:", scenario.name)
				t.Logf("- Total Connections: %d", metrics.TotalConnections)
				t.Logf("- Active Connections: %d", metrics.ActiveConnections)
				t.Logf("- Idle Connections: %d", metrics.IdleConnections)

				// Verify recovery with timeout manager
				err := rs.timeoutManager.ExecuteWithTimeout(verifyCtx, "SET", 1,
					func(ctx context.Context) error {
						return rs.Set(ctx, "recovery_verification", "recovery_test", time.Hour)
					})
				assert.NoError(t, err, "Pool should be functional after recovery")
			})
		}
	})

	t.Run("Concurrent Health Monitoring", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t,
			WithInitialConfig(func(cfg *config.Config) {
				cfg.Pool.Status = true
				cfg.Pool.Size = 30
				cfg.Pool.MinIdle = 5
				cfg.Redis.HealthCheckInterval = 1
			}),
		)
		defer cancel()

		// Track health metrics during concurrent operations
		type healthMetric struct {
			timestamp   time.Time
			connections int64
			errors      int64
		}

		var metrics []healthMetric
		var metricsMutex sync.Mutex

		// Start health monitoring
		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					currentMetrics := rs.poolManager.getCurrentMetrics()
					if currentMetrics != nil {
						metricsMutex.Lock()
						metrics = append(metrics, healthMetric{
							timestamp:   time.Now(),
							connections: currentMetrics.TotalConnections,
							errors:      currentMetrics.ErrorCount,
						})
						metricsMutex.Unlock()
					}
				}
			}
		}()

		// Generate concurrent load
		var wg sync.WaitGroup
		operations := 100
		concurrency := 10
		sem := make(chan struct{}, concurrency)

		for i := 0; i < operations; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				sem <- struct{}{}        // Acquire semaphore
				defer func() { <-sem }() // Release semaphore

				key := fmt.Sprintf("concurrent_health_key_%d", id)
				value := fmt.Sprintf("value_%d", id)

				// Perform multiple operations
				for j := 0; j < 3; j++ {
					if err := rs.Set(ctx, key, value, time.Hour); err != nil {
						t.Logf("Set error: %v", err)
						continue
					}

					if _, err := rs.Get(ctx, key); err != nil {
						t.Logf("Get error: %v", err)
						continue
					}

					// Random delay between operations
					time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()
		close(done)

		// Analyze health metrics
		metricsMutex.Lock()
		defer metricsMutex.Unlock()

		require.NotEmpty(t, metrics, "Should have collected health metrics")

		var totalConns int64
		var maxConns int64
		var totalErrors int64

		for _, m := range metrics {
			totalConns += m.connections
			if m.connections > maxConns {
				maxConns = m.connections
			}
			totalErrors += m.errors
		}

		avgConns := totalConns / int64(len(metrics))

		// Log metrics summary
		t.Logf("Health Monitoring Summary:")
		t.Logf("- Average connections: %d", avgConns)
		t.Logf("- Maximum connections: %d", maxConns)
		t.Logf("- Total errors: %d", totalErrors)
		t.Logf("- Metric samples: %d", len(metrics))

		// Assertions
		assert.LessOrEqual(t, maxConns, int64(rs.cfg.Pool.Size),
			"Should not exceed maximum pool size")
		assert.GreaterOrEqual(t, avgConns, int64(rs.cfg.Pool.MinIdle),
			"Should maintain minimum connections on average")
		assert.Less(t, totalErrors, int64(operations/10),
			"Error rate should be reasonable")
	})
}
