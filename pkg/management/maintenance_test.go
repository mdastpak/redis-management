package management

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceManager(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	t.Run("Enable and Disable Maintenance", func(t *testing.T) {
		mm := NewMaintenanceManager(service)
		ctx := context.Background()

		// Enable maintenance mode
		err := mm.EnableMaintenance(ctx, time.Hour, "scheduled maintenance", true)
		require.NoError(t, err)

		// Check status
		status := mm.GetMaintenanceStatus()
		assert.True(t, status.IsMaintenanceMode)
		assert.True(t, status.ReadOnlyMode)
		assert.Equal(t, "scheduled maintenance", status.Reason)

		// Verify operation permissions
		assert.True(t, mm.IsOperationAllowed("GET"))
		assert.False(t, mm.IsOperationAllowed("SET"))

		// Disable maintenance mode
		err = mm.DisableMaintenance()
		require.NoError(t, err)

		// Verify status after disable
		status = mm.GetMaintenanceStatus()
		assert.False(t, status.IsMaintenanceMode)
		assert.False(t, status.ReadOnlyMode)
	})

	t.Run("Auto Disable After Duration", func(t *testing.T) {
		mm := NewMaintenanceManager(service)
		ctx := context.Background()

		// Enable maintenance for 1 second
		err := mm.EnableMaintenance(ctx, time.Second, "short maintenance", true)
		require.NoError(t, err)

		// Verify initial state
		assert.True(t, mm.GetMaintenanceStatus().IsMaintenanceMode)

		// Wait for auto-disable
		time.Sleep(1100 * time.Millisecond)

		// Verify maintenance mode was disabled
		assert.False(t, mm.GetMaintenanceStatus().IsMaintenanceMode)
	})

	t.Run("Operation Permissions During Maintenance", func(t *testing.T) {
		mm := NewMaintenanceManager(service)
		ctx := context.Background()

		// Enable maintenance in read-only mode
		err := mm.EnableMaintenance(ctx, time.Hour, "testing permissions", true)
		require.NoError(t, err)

		// Test various operations
		readOps := []string{"GET", "MGET", "EXISTS", "TTL", "PING"}
		writeOps := []string{"SET", "DEL", "INCR", "EXPIRE"}

		for _, op := range readOps {
			assert.True(t, mm.IsOperationAllowed(op), "Read operation should be allowed: %s", op)
		}

		for _, op := range writeOps {
			assert.False(t, mm.IsOperationAllowed(op), "Write operation should be blocked: %s", op)
		}
	})

	t.Run("Operations During Maintenance", func(t *testing.T) {
		mm := NewMaintenanceManager(service)
		ctx := context.Background()

		// Enable maintenance mode
		err := mm.EnableMaintenance(ctx, time.Hour, "test maintenance", true)
		require.NoError(t, err)

		// Try operations
		key := "test_key"
		value := "test_value"

		// Write operation should fail
		err = service.Set(ctx, key, value, 0)
		assert.Error(t, err, "Write operation should fail during maintenance")

		// Read operation should succeed
		_, err = service.Get(ctx, key)
		assert.NoError(t, err, "Read operation should succeed during maintenance")

		// Cleanup
		mm.DisableMaintenance()
	})
}
