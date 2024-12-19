package management

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisServiceReload(t *testing.T) {
	t.Parallel()
	t.Run("Basic Configuration Reload", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// Modify configuration
		newCfg := *rs.cfg

		newCfg.Pool.Status = true
		newCfg.Pool.Size = 15

		newCfg.Redis.KeyPrefix = "basic_configuration_reload:"

		// Reload configuration
		err := rs.ReloadConfig(&newCfg)
		require.NoError(t, err)

		// Verify changes
		require.NotNil(t, rs.pool, "Pool should be initialized")
		assert.Equal(t, newCfg.Pool.Size, rs.cfg.Pool.Size)
		assert.Equal(t, newCfg.Redis.KeyPrefix, rs.cfg.Redis.KeyPrefix)

		// Verify service still works
		err = rs.Set(ctx, "test_key", "test_value", time.Hour)
		assert.NoError(t, err)

		t.Cleanup(func() {
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
		})
	})

	t.Run("Reload With Invalid Configuration", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// Store original configuration
		originalCfg := *rs.cfg

		// Create invalid configuration
		invalidCfg := *rs.cfg
		invalidCfg.Pool.Size = -1 // Invalid pool size
		invalidCfg.Redis.KeyPrefix = "reload-with_invalid_configuration:"

		// Attempt reload
		err := rs.ReloadConfig(&invalidCfg)
		assert.Error(t, err)

		// Verify original configuration is preserved
		assert.Equal(t, originalCfg.Pool.Size, rs.cfg.Pool.Size)

		// Verify service still works
		err = rs.Set(ctx, "test_key", "test_value", time.Hour)
		assert.NoError(t, err)

		t.Cleanup(func() {
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
		})
	})

	t.Run("Reload During Active Operations", func(t *testing.T) {
		rs, ctx, cancel := setupTestRedisWithConfig(t)
		defer cancel()

		// Start some operations
		doneChan := make(chan struct{})
		go func() {
			defer close(doneChan)
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("concurrent_key_%d", i)
				err := rs.Set(ctx, key, "value", time.Hour)
				if err != nil {
					t.Logf("Reload_During_Active_Operations Operation error: %v", err)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		// Wait briefly for operations to start
		time.Sleep(100 * time.Millisecond)

		// Modify and reload configuration
		newCfg := *rs.cfg
		newCfg.Pool.Size = rs.cfg.Pool.Size * 2

		err := rs.ReloadConfig(&newCfg)
		require.NoError(t, err)

		// Wait for operations to complete
		<-doneChan

		// Verify service is still operational
		err = rs.Set(ctx, "final_test", "value", time.Hour)
		assert.NoError(t, err)

		t.Cleanup(func() {
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
		})
	})

	t.Run("Reload Component Specific Changes", func(t *testing.T) {

		t.Run("Pool Changes", func(t *testing.T) {
			rs, _, cancel := setupTestRedisWithConfig(t)
			defer cancel()

			newCfg := *rs.cfg
			newCfg.Pool.Size = rs.cfg.Pool.Size * 2

			err := rs.ReloadConfig(&newCfg)
			assert.NoError(t, err)
			assert.Equal(t, newCfg.Pool.Size, rs.cfg.Pool.Size)
		})

		t.Run("Circuit Breaker Changes", func(t *testing.T) {
			rs, _, cancel := setupTestRedisWithConfig(t)
			defer cancel()

			newCfg := *rs.cfg
			newCfg.Circuit.Status = !rs.cfg.Circuit.Status

			err := rs.ReloadConfig(&newCfg)
			assert.NoError(t, err)
			assert.Equal(t, newCfg.Circuit.Status, rs.cfg.Circuit.Status)
		})
	})
}

func TestReloadStateHelpers(t *testing.T) {
	t.Run("No Changes", func(t *testing.T) {
		state := reloadState{}
		assert.False(t, state.hasChanges())
	})

	t.Run("With Changes", func(t *testing.T) {
		state := reloadState{
			needsPoolReload: true,
		}
		assert.True(t, state.hasChanges())
	})

	t.Run("String Representation", func(t *testing.T) {
		state := reloadState{
			needsPoolReload: true,
		}
		expected := "Pool: true, KeyMgr: false, Circuit: false, Operation: false"
		assert.Equal(t, expected, state.String())
	})
}
