package management

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShutdownManager(t *testing.T) {
	t.Parallel()

	t.Run("Basic Shutdown", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		sm := NewShutdownManager(rs, 5*time.Second)
		assert.False(t, sm.IsShuttingDown())

		err = sm.Shutdown(ctx)
		require.NoError(t, err)
		assert.True(t, sm.IsShuttingDown())
	})

	t.Run("Operation Tracking", func(t *testing.T) {
		// ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		sm := NewShutdownManager(rs, 5*time.Second)

		// Track some operations
		for i := 0; i < 5; i++ {
			allowed := sm.TrackOperation()
			assert.True(t, allowed)
		}
		assert.Equal(t, int64(5), sm.GetOperationCount())

		// Complete operations
		for i := 0; i < 5; i++ {
			sm.FinishOperation()
		}
		assert.Equal(t, int64(0), sm.GetOperationCount())
	})

	t.Run("Shutdown with Active Operations", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		sm := NewShutdownManager(rs, 1*time.Second)

		// Start some long-running operations
		for i := 0; i < 3; i++ {
			sm.TrackOperation()
		}

		// Attempt shutdown
		err = sm.Shutdown(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "shutdown timed out")
	})

	t.Run("Prevent New Operations During Shutdown", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		sm := NewShutdownManager(rs, 5*time.Second)

		// Start shutdown
		go func() {
			_ = sm.Shutdown(ctx)
		}()

		// Wait for shutdown to begin
		time.Sleep(100 * time.Millisecond)

		// Attempt new operation
		allowed := sm.TrackOperation()
		assert.False(t, allowed, "Should not allow new operations during shutdown")
	})

	t.Run("Graceful Shutdown", func(t *testing.T) {
		ctx := context.Background()

		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		sm := NewShutdownManager(rs, 5*time.Second)
		operationDone := make(chan struct{})

		// Start a tracked operation
		assert.True(t, sm.TrackOperation())

		// Complete operation after delay
		go func() {
			time.Sleep(500 * time.Millisecond)
			sm.FinishOperation()
			close(operationDone)
		}()

		// Initiate shutdown
		err = sm.Shutdown(ctx)
		require.NoError(t, err)

		// Verify operation completed
		select {
		case <-operationDone:
			assert.Equal(t, int64(0), sm.GetOperationCount())
		case <-time.After(1 * time.Second):
			t.Fatal("Operation did not complete")
		}
	})
}
