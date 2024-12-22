package management

import (
	"context"
	"fmt"
	"redis-management/config"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolScaling(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		initialSize     int
		maxConnections  int
		loadMultiplier  int
		expectedMinSize int
		expectedMaxSize int
		duration        time.Duration
		cooldown        time.Duration
		expectedRecords int
	}{
		{
			name:            "Gradual Scale Up",
			initialSize:     10,
			maxConnections:  30,
			loadMultiplier:  3,
			expectedMinSize: 15,
			expectedMaxSize: 25,
			duration:        3 * time.Second,
			cooldown:        1 * time.Second,
			expectedRecords: 30,
		},
		{
			name:            "Rapid Scale Up",
			initialSize:     20,
			maxConnections:  50,
			loadMultiplier:  5,
			expectedMinSize: 30,
			expectedMaxSize: 45,
			duration:        3 * time.Second,
			cooldown:        1 * time.Second,
			expectedRecords: 100,
		},
		{
			name:            "Scale Under Light Load",
			initialSize:     15,
			maxConnections:  25,
			loadMultiplier:  2,
			expectedMinSize: 15,
			expectedMaxSize: 20,
			duration:        2 * time.Second,
			cooldown:        1 * time.Second,
			expectedRecords: 30,
		},
		{
			name:            "Scale Under Heavy Load",
			initialSize:     40,
			maxConnections:  100,
			loadMultiplier:  10,
			expectedMinSize: 60,
			expectedMaxSize: 90,
			duration:        5 * time.Second,
			cooldown:        2 * time.Second,
			expectedRecords: 400,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Initialize service configuration with proper timeouts
			cfg := config.Config{
				Pool: config.PoolConfig{
					Status:      true,
					Size:        tc.initialSize,
					MinIdle:     tc.initialSize / 2,
					MaxIdleTime: 60,
					WaitTimeout: 30,
				},
				Timeout: config.TimeoutConfig{
					BaseTimeout:   5 * time.Second,
					MaxTimeout:    30 * time.Second,
					MinTimeout:    100 * time.Millisecond,
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
				},
			}

			// Create main test context with proper timeout
			ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout.MaxTimeout)
			defer cancel()

			// Create cleanup context with proper timeout
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(),
				cfg.Redis.ShutdownTimeout+cfg.Timeout.Operations.BulkBase,
			)
			defer cleanupCancel()

			// Initialize Redis service with timeouts
			rs, _, closeFunc := setupTestRedisWithConfig(t,
				WithInitialConfig(func(c *config.Config) {
					c.Redis.ShutdownTimeout = 10 * time.Second
					c.Redis.RetryAttempts = 3
					c.Redis.RetryDelay = time.Second
					c.Redis.MaxRetryBackoff = 5 * time.Second

					c.Pool = cfg.Pool
					c.Timeout = cfg.Timeout
				}),
			)

			// Create cleanup function using existing context
			cleanup := func() {
				if err := rs.Close(cleanupCtx); err != nil {
					t.Logf("Error during service cleanup: %v", err)
				}
				closeFunc()
			}
			defer cleanup()

			// Track test keys with timeouts
			var keys []string
			var keysMutex sync.Mutex
			defer func() {
				deleteTimeout := cfg.Timeout.Operations.Delete * time.Duration(len(keys))
				if deleteTimeout > cfg.Timeout.MaxTimeout {
					deleteTimeout = cfg.Timeout.MaxTimeout
				}

				delCtx, cancel := context.WithTimeout(context.Background(), deleteTimeout)
				defer cancel()

				keysMutex.Lock()
				for _, key := range keys {
					// Use BulkDelete for better performance
					if err := rs.Delete(delCtx, key); err != nil {
						t.Logf("Failed to cleanup key %s: %v", key, err)
					}
				}
				keysMutex.Unlock()
			}()

			// Create operation timeouts
			opTimeout := cfg.Timeout.Operations.Set
			if opTimeout < cfg.Timeout.MinTimeout {
				opTimeout = cfg.Timeout.MinTimeout
			}

			bulkOpTimeout := cfg.Timeout.Operations.BulkBase *
				time.Duration(tc.loadMultiplier*tc.initialSize)
			if bulkOpTimeout > cfg.Timeout.MaxTimeout {
				bulkOpTimeout = cfg.Timeout.MaxTimeout
			}

			// Monitor metrics with configured interval
			metricsCtx, metricsCancel := context.WithTimeout(ctx, bulkOpTimeout)
			defer metricsCancel() // Add this to ensure metricsCancel is always called

			var metrics []*PoolMetrics
			var metricsMutex sync.Mutex
			var metricsWg sync.WaitGroup
			metricsWg.Add(1)

			go func() {
				defer metricsWg.Done()
				ticker := time.NewTicker(cfg.Timeout.MinTimeout)
				defer ticker.Stop()

				for {
					select {
					case <-metricsCtx.Done():
						return
					case <-ticker.C:
						if m := rs.poolManager.getCurrentMetrics(); m != nil {
							metricsMutex.Lock()
							metrics = append(metrics, m)
							metricsMutex.Unlock()
						}
					}
				}
			}()

			// Generate load with timeouts
			var opsWg sync.WaitGroup
			sem := make(chan struct{}, tc.maxConnections/2)
			recordCount := int32(0)

			startTime := time.Now()
			for i := 0; i < tc.loadMultiplier*tc.initialSize &&
				time.Since(startTime) < tc.duration; i++ {

				select {
				case <-ctx.Done():
					t.Log("Context cancelled during load generation")
					metricsWg.Wait() // Ensure metrics goroutine is finished
					return
				default:
					opsWg.Add(1)
					go func(id int) {
						defer opsWg.Done()
						sem <- struct{}{}
						defer func() { <-sem }()

						// Create operation context with timeout
						opCtx, cancel := context.WithTimeout(ctx, opTimeout)
						defer cancel()

						key := fmt.Sprintf("%s_key_%d", tc.name, id)
						value := fmt.Sprintf("value_%d", id)

						if err := rs.Set(opCtx, key, value, time.Hour); err != nil {
							t.Logf("Operation error for key %s: %v", key, err)
							return
						}

						keysMutex.Lock()
						keys = append(keys, key)
						keysMutex.Unlock()
						atomic.AddInt32(&recordCount, 1)
					}(i)
				}
			}

			// Wait for operations with configured timeout
			waitChan := make(chan struct{})
			go func() {
				opsWg.Wait()
				close(waitChan)
			}()

			select {
			case <-waitChan:
				t.Log("All operations completed successfully")
			case <-time.After(bulkOpTimeout):
				t.Error("Timeout waiting for operations")
				return
			}

			// Stop metrics collection
			metricsCancel()
			metricsWg.Wait()

			// Allow for cooldown using configured timeout
			time.Sleep(cfg.Timeout.MinTimeout)

			// Final metrics check
			metricsMutex.Lock()
			if len(metrics) > 0 {
				finalMetrics := metrics[len(metrics)-1]
				t.Logf("%s Final Metrics:", tc.name)
				t.Logf("- Total Connections: %d", finalMetrics.TotalConnections)
				t.Logf("- Active Connections: %d", finalMetrics.ActiveConnections)
				t.Logf("- Idle Connections: %d", finalMetrics.IdleConnections)
				t.Logf("- Records Created: %d", atomic.LoadInt32(&recordCount))

				// Assertions
				assert.GreaterOrEqual(t, finalMetrics.TotalConnections,
					int64(tc.expectedMinSize))
				assert.LessOrEqual(t, finalMetrics.TotalConnections,
					int64(tc.expectedMaxSize))
				assert.InDelta(t, tc.expectedRecords,
					atomic.LoadInt32(&recordCount),
					float64(tc.expectedRecords)*0.2)
			}
			metricsMutex.Unlock()
		})
	}
}
