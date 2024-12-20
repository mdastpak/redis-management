package management

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdownManager(t *testing.T) {
	t.Parallel()

	t.Run("Basic Shutdown", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		err := rs.operationManager.shutdownManager.Shutdown(ctx)
		require.NoError(t, err)
		assert.True(t, rs.operationManager.shutdownManager.IsShuttingDown())

	})

	t.Run("Operation Tracking", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// Track some operations
		for i := 0; i < 5; i++ {
			allowed := rs.operationManager.shutdownManager.TrackOperation(ctx)
			assert.True(t, allowed)
		}
		assert.Equal(t, int64(5), rs.operationManager.shutdownManager.GetOperationCount())

		// Complete operations
		for i := 0; i < 5; i++ {
			rs.operationManager.shutdownManager.FinishOperation()
		}
		assert.Equal(t, int64(0), rs.operationManager.shutdownManager.GetOperationCount())
	})

	t.Run("Graceful Shutdown with Active Operations", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		operationDone := make(chan struct{})

		// Start a tracked operation
		assert.True(t, rs.operationManager.shutdownManager.TrackOperation(ctx))

		// Start shutdown in background
		shutdownComplete := make(chan error)
		go func() {
			shutdownComplete <- rs.operationManager.shutdownManager.Shutdown(ctx)
		}()

		// Complete operation after delay
		go func() {
			time.Sleep(200 * time.Millisecond)
			rs.operationManager.shutdownManager.FinishOperation()
			close(operationDone)
		}()

		// Wait for operation completion
		select {
		case <-operationDone:
			// Operation completed
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Operation did not complete in time")
		}

		// Check shutdown result
		err := <-shutdownComplete
		require.NoError(t, err)
		assert.Equal(t, int64(0), rs.operationManager.shutdownManager.GetOperationCount())
	})

	t.Run("Concurrent Shutdown Requests", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// Track an operation
		assert.True(t, rs.operationManager.shutdownManager.TrackOperation(ctx))

		// Launch multiple shutdown requests
		var wg sync.WaitGroup
		errs := make(chan error, 3)

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- rs.operationManager.shutdownManager.Shutdown(ctx)
			}()
		}

		// Complete the operation
		time.Sleep(100 * time.Millisecond)
		rs.operationManager.shutdownManager.FinishOperation()

		// Wait for all shutdown requests
		wg.Wait()
		close(errs)

		// All shutdown requests should complete successfully
		for err := range errs {
			require.NoError(t, err)
		}
	})

	t.Run("Shutdown Timeout", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// Start long-running operations
		for i := 0; i < 3; i++ {
			assert.True(t, rs.operationManager.shutdownManager.TrackOperation(ctx))
		}

		// Attempt shutdown with short timeout
		err := rs.operationManager.shutdownManager.Shutdown(ctx)
		require.Error(t, err)

		// Detailed error logging
		// t.Logf("Error type: %T", err)
		// t.Logf("Full error message: %q", err.Error())
		// t.Logf("Error value: %#v", err)

		assert.Contains(t, err.Error(), "shutdown timed out with 3 operations remaining")
	})

	t.Run("Prevent New Operations During Shutdown", func(t *testing.T) {
		ctx := context.Background()
		rs, err := setupTestRedis(ctx)
		require.NoError(t, err)

		// Start shutdown
		go func() {
			err := rs.operationManager.shutdownManager.Shutdown(ctx)
			require.NoError(t, err)
		}()

		// Wait for shutdown to begin
		time.Sleep(50 * time.Millisecond)

		// Try to start new operation
		ok := rs.operationManager.shutdownManager.TrackOperation(ctx)
		assert.False(t, ok, "Should not allow new operations during shutdown")
	})

	t.Run("Graceful Shutdown", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		operationDone := make(chan struct{})

		// Start a tracked operation
		assert.True(t, rs.operationManager.shutdownManager.TrackOperation(ctx))

		// Complete operation after delay
		go func() {
			time.Sleep(500 * time.Millisecond)
			rs.operationManager.shutdownManager.FinishOperation()
			close(operationDone)
		}()

		// Initiate shutdown
		err := rs.operationManager.shutdownManager.Shutdown(ctx)
		require.NoError(t, err)

		// Verify operation completed
		select {
		case <-operationDone:
			assert.Equal(t, int64(0), rs.operationManager.shutdownManager.GetOperationCount())
		case <-time.After(1 * time.Second):
			t.Fatal("Operation did not complete")
		}
	})
}
