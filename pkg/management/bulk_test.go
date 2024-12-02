// pkg/management/bulk_test.go
package management

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mdastpak/redis-management/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkOperations(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	// Log initial config
	// t.Logf("Starting test with Default config: %+v", cfg.Bulk)

	cfg.Bulk = config.BulkConfig{
		Status:          true,
		BatchSize:       10,
		FlushInterval:   100,
		MaxRetries:      1,
		ConcurrentFlush: true,
	}

	// Log initial config
	// t.Logf("Starting test with Manupulated config: %+v", cfg.Bulk)

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	t.Run("Bulk Set Operations", func(t *testing.T) {
		numOperations := 10
		var wg sync.WaitGroup
		errChan := make(chan error, numOperations)
		doneChan := make(chan bool)

		// Time Duration for the test
		// start := time.Now()
		// t.Logf("Starting bulk operations at: %v", start)

		// Execute operations
		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("bulk:key:%d", i)
				value := fmt.Sprintf("value:%d", i)

				// t.Logf("Adding operation %d - Key: %s, Value: %s", i, key, value)

				if err := service.AddBulkOperation(context.Background(), "SET", key, value, time.Hour); err != nil {
					// t.Logf("Error in operation %d: %v", i, err)
					select {
					case errChan <- fmt.Errorf("op %d: %v", i, err):
					default:
					}
				}
				// } else {
				// 	t.Logf("Successfully added operation %d", i)
				// }

			}(i)
		}

		// انتظار برای تکمیل
		go func() {
			wg.Wait()
			close(doneChan)
		}()

		// منتظر تکمیل یا timeout
		select {
		case <-doneChan:
			// بررسی نتایج
			close(errChan)
			for err := range errChan {
				t.Error(err)
			}

			// elapsed := time.Since(start)
			// t.Logf("Bulk operations completed in %v", elapsed)

			// verifyCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			// defer cancel()

			// Log all keys in Redis
			// keys := mr.Keys()
			// t.Logf("Total keys in Redis: %d", len(keys))
			// for _, key := range keys {
			// 	value, err := mr.Get(key)
			// 	if err != nil {
			// 		t.Logf("Error getting key %s: %v", key, err)
			// 	} else {
			// 		t.Logf("Found key: %s with value: %s", key, value)
			// 	}
			// }

			// assert.Equal(t, numOperations, len(keys),
			// 	"Expected %d keys, got %d", numOperations, len(keys))

			// Verify results
			for i := 0; i < numOperations; i++ {
				key := fmt.Sprintf("test:bulk:key:%d", i)
				expectedValue := fmt.Sprintf("value:%d", i)

				value, err := mr.Get(key)
				if err != nil {
					t.Logf("Verification failed for key %s: %v", key, err)
					t.Errorf("Failed to get key %s: %v", key, err)
					continue
				}

				if value != expectedValue {
					t.Logf("Value mismatch for key %s - Expected: %s, Got: %s",
						key, expectedValue, value)
				}

				assert.Equal(t, expectedValue, value,
					"Unexpected value for key %s", key)
			}

		case <-time.After(3 * time.Second):
			t.Fatal("Bulk operations timed out")
		}
	})
}
