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
}

// pkg/management/maintenance_test.go
func TestMaintenanceOperations(t *testing.T) {
	t.Run("Operations During Maintenance", func(t *testing.T) {
		// Setup
		mr, cfg := setupTestRedis(t)
		t.Cleanup(func() {
			mr.Close()
		})

		service, err := NewRedisService(cfg)
		require.NoError(t, err)
		t.Cleanup(func() {
			service.Close()
		})

		ctx := context.Background()

		// Initialize test data
		key := "test_key"
		value := "test_value"
		err = service.Set(ctx, key, value, time.Hour)
		require.NoError(t, err)

		// Enable maintenance mode
		err = service.operationManager.GetMaintenanceManager().EnableMaintenance(ctx, time.Hour, "test maintenance", true)
		require.NoError(t, err)

		// Verify maintenance status
		status := service.operationManager.GetMaintenanceManager().GetMaintenanceStatus()
		require.True(t, status.IsMaintenanceMode)
		require.True(t, status.ReadOnlyMode)
		require.Equal(t, "test maintenance", status.Reason)

		t.Run("Write Operations", func(t *testing.T) {
			testCases := []struct {
				name      string
				operation func() error
			}{
				{
					name: "Set Operation",
					operation: func() error {
						return service.Set(ctx, "new_key", "new_value", 0)
					},
				},
				{
					name: "Delete Operation",
					operation: func() error {
						return service.Delete(ctx, key)
					},
				},
				{
					name: "SetTTL Operation",
					operation: func() error {
						return service.SetTTL(ctx, key, time.Hour)
					},
				},
				{
					name: "Batch Write Operation",
					operation: func() error {
						return service.SetBatch(ctx, map[string]interface{}{
							"batch_key1": "value1",
							"batch_key2": "value2",
						}, 0)
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					err := tc.operation()
					assert.Error(t, err)
					assert.Contains(t, err.Error(), "not allowed during maintenance mode")
				})
			}
		})

		t.Run("Read Operations", func(t *testing.T) {
			testCases := []struct {
				name      string
				operation func() error
			}{
				{
					name: "Get Operation",
					operation: func() error {
						_, err := service.Get(ctx, key)
						return err
					},
				},
				{
					name: "GetTTL Operation",
					operation: func() error {
						_, err := service.GetTTL(ctx, key)
						return err
					},
				},
				{
					name: "Batch Read Operation",
					operation: func() error {
						_, err := service.GetBatch(ctx, []string{key})
						return err
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					err := tc.operation()
					assert.NoError(t, err)
				})
			}
		})

		t.Run("Disable Maintenance", func(t *testing.T) {
			err := service.operationManager.GetMaintenanceManager().DisableMaintenance()
			require.NoError(t, err)

			status := service.operationManager.GetMaintenanceManager().GetMaintenanceStatus()
			assert.False(t, status.IsMaintenanceMode)

			err = service.Set(ctx, key, "new_value", 0)
			assert.NoError(t, err)
		})
	})
}
