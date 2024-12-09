package management

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperationManager(t *testing.T) {
	t.Run("Operation Execution During Maintenance", func(t *testing.T) {
		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		om := NewOperationManager(rs)
		ctx := context.Background()

		// Enable maintenance mode
		maintManager := om.GetMaintenanceManager()
		err = maintManager.EnableMaintenance(ctx, time.Hour, "test maintenance", true)
		require.NoError(t, err)

		// Test read operation
		_, err = om.ExecuteReadOp(ctx, "GET", func() (string, error) {
			return "test", nil
		})
		assert.NoError(t, err, "Read operation should be allowed")

		// Test write operation
		err = om.ExecuteWithLock(ctx, "SET", func() error {
			return nil
		})
		assert.Error(t, err, "Write operation should be blocked")

		// Disable maintenance
		err = maintManager.DisableMaintenance()
		require.NoError(t, err)

		// Test operation after maintenance
		err = om.ExecuteWithLock(ctx, "SET", func() error {
			return nil
		})
		assert.NoError(t, err, "Operation should be allowed after maintenance")
	})

	t.Run("Operation Execution During Shutdown", func(t *testing.T) {
		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		om := NewOperationManager(rs)
		ctx := context.Background()

		// Start shutdown
		go func() {
			time.Sleep(100 * time.Millisecond)
			err := om.GetShutdownManager().Shutdown(ctx)
			require.NoError(t, err)
		}()

		// Try operations
		time.Sleep(200 * time.Millisecond)

		_, err = om.ExecuteReadOp(ctx, "GET", func() (string, error) {
			return "test", nil
		})
		assert.Error(t, err, "Operation should be blocked during shutdown")
	})

	t.Run("Batch Operations", func(t *testing.T) {
		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		om := NewOperationManager(rs)
		ctx := context.Background()

		// Test batch string operation
		result, err := om.ExecuteBatchOp(ctx, "MGET", func() (map[string]string, error) {
			return map[string]string{"key": "value"}, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "value", result["key"])

		// Test batch duration operation
		durations, err := om.ExecuteBatchDurationOp(ctx, "TTL", func() (map[string]time.Duration, error) {
			return map[string]time.Duration{"key": time.Second}, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, time.Second, durations["key"])
	})

	t.Run("Status Reporting", func(t *testing.T) {
		rs, err := setupTestRedis()
		require.NoError(t, err)
		defer rs.Close()

		om := NewOperationManager(rs)
		ctx := context.Background()

		// Enable maintenance mode
		maintManager := om.GetMaintenanceManager()
		err = maintManager.EnableMaintenance(ctx, time.Hour, "test status", true)
		require.NoError(t, err)

		// Check status
		status := om.GetStatus()
		assert.True(t, status.IsMaintenanceMode)
		assert.False(t, status.IsShuttingDown)
		assert.Equal(t, "test status", status.MaintenanceReason)
		assert.True(t, status.ReadOnlyMode)
		assert.Greater(t, status.RemainingTime, time.Duration(0))
	})
}
