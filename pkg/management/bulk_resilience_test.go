package management

import (
	"fmt"
	"math/rand"
	"redis-management/config"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBulkOperationsResilience(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		itemCount     int
		failureRate   float64
		retryAttempts int
		expectSuccess bool
	}{
		{
			name:          "High Failure Rate",
			itemCount:     300,
			failureRate:   0.6,
			retryAttempts: 3,
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup test Redis service
			rs, ctx, cancel := setupTestRedisWithConfig(t,
				WithInitialConfig(func(cfg *config.Config) {
					cfg.Redis.HashKeys = false
					cfg.Redis.KeyPrefix = fmt.Sprintf("resilience_test_%s:", tc.name)
				}))
			defer cancel()

			// Create test data with some keys that will deliberately fail
			items := make(map[string]interface{}, tc.itemCount)
			failureKeys := make(map[string]bool)
			failureCount := 0

			for i := 0; i < tc.itemCount; i++ {
				key := fmt.Sprintf("key_%d", i)
				items[key] = fmt.Sprintf("value_%d", i)

				// Mark some keys to fail based on failure rate
				if rand.Float64() < tc.failureRate {
					failureKeys[key] = true
					failureCount++
				}
			}

			// Execute bulk operation with retry logic
			var finalErr error
			var retryCount atomic.Int32

			for attempt := 0; attempt < tc.retryAttempts; attempt++ {
				// Process all items but track failures
				result := &BulkResult{}
				failedKeys := []string{}
				errors := []error{}

				for key, value := range items {
					if failureKeys[key] {
						failedKeys = append(failedKeys, key)
						errors = append(errors, fmt.Errorf("simulated failure for key %s", key))
					} else {
						err := rs.Set(ctx, key, value, time.Hour)
						if err != nil {
							failedKeys = append(failedKeys, key)
							errors = append(errors, err)
						} else {
							result.SuccessCount++
						}
					}
				}

				// Update result with failures
				result.FailedKeys = failedKeys
				result.Errors = errors

				// Update metrics
				rs.updateBulkMetrics(result, time.Millisecond*100)

				if len(result.Errors) > 0 {
					finalErr = fmt.Errorf("bulk operation failed for %d items", len(result.Errors))
					retryCount.Add(1)

					// Log retry attempt
					t.Logf("Retry attempt %d/%d failed: %v", attempt+1, tc.retryAttempts, finalErr)

					// Get metrics for this attempt
					metrics := rs.GetBulkMetrics()
					t.Logf("Metrics after attempt %d:", attempt+1)
					t.Logf("- Operations: %d", metrics.OperationsCount.Load())
					t.Logf("- Successes: %d", metrics.SuccessCount.Load())
					t.Logf("- Errors: %d", metrics.ErrorCount.Load())

					// Exponential backoff
					backoff := time.Duration(attempt+1) * 100 * time.Millisecond
					t.Logf("Waiting %v before next attempt", backoff)
					time.Sleep(backoff)
				} else {
					finalErr = nil
					break
				}
			}

			// Get final metrics
			finalMetrics := rs.GetBulkMetrics()

			// Log results
			t.Logf("Resilience Test Results for %s:", tc.name)
			t.Logf("- Simulated Failures: %d", failureCount)
			t.Logf("- Retry Attempts: %d", retryCount.Load())
			t.Logf("- Total Operations: %d", finalMetrics.OperationsCount.Load())
			t.Logf("- Success Count: %d", finalMetrics.SuccessCount.Load())
			t.Logf("- Error Count: %d", finalMetrics.ErrorCount.Load())

			if avgLatency, ok := finalMetrics.AverageLatency.Load().(time.Duration); ok {
				t.Logf("- Average Latency: %v", avgLatency)
			}

			actualFailureRate := float64(failureCount) / float64(tc.itemCount)
			t.Logf("Actual failure rate: %.2f (expected: %.2f)", actualFailureRate, tc.failureRate)

			if tc.expectSuccess {
				assert.NoError(t, finalErr, "Expected successful operation after retries")
				assert.Greater(t, finalMetrics.SuccessCount.Load(), int64(0),
					"Should have some successful operations")
			} else {
				assert.Error(t, finalErr, "Expected operation to fail even after retries")
				assert.Greater(t, failureCount, 0,
					"Should have some simulated failures")
				assert.Greater(t, finalMetrics.ErrorCount.Load(), int64(0),
					"Should have some failed operations")
			}

			assert.InDelta(t, tc.failureRate, actualFailureRate, 0.2,
				"Actual failure rate should be close to expected")
		})
	}
}
