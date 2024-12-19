package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeoutConfigBasic(t *testing.T) {

	t.Run("Default Configuration", func(t *testing.T) {
		cfg, err := Load()
		require.NoError(t, err)
		assert.NotNil(t, cfg.Timeout)

		// Verify default values
		assert.Equal(t, 5*time.Second, cfg.Timeout.BaseTimeout)
		assert.Equal(t, 30*time.Second, cfg.Timeout.MaxTimeout)
		assert.Equal(t, 100*time.Millisecond, cfg.Timeout.MinTimeout)
		assert.Equal(t, 1.5, cfg.Timeout.BackoffFactor)
	})

	t.Run("Default Operation Timeouts", func(t *testing.T) {
		cfg, err := Load()
		require.NoError(t, err)

		assert.Equal(t, time.Second, cfg.Timeout.Operations.Get)
		assert.Equal(t, 2*time.Second, cfg.Timeout.Operations.Set)
		assert.Equal(t, 2*time.Second, cfg.Timeout.Operations.Delete)
		assert.Equal(t, 5*time.Second, cfg.Timeout.Operations.BulkBase)
	})

	t.Run("Default Adaptive Settings", func(t *testing.T) {
		cfg, err := Load()
		require.NoError(t, err)

		assert.True(t, cfg.Timeout.Adaptive.Enabled)
		assert.Equal(t, 100, cfg.Timeout.Adaptive.WindowSize)
		assert.Equal(t, 0.2, cfg.Timeout.Adaptive.AdjustmentThreshold)
		assert.Equal(t, 0.5, cfg.Timeout.Adaptive.MaxAdjustment)
		assert.Equal(t, 24*time.Hour, cfg.Timeout.Adaptive.HistoryRetention)
	})

	t.Run("Environment Variable Override", func(t *testing.T) {
		t.Setenv("RDS_MGMNT_TIMEOUT_BASE_TIMEOUT", "10s")
		t.Setenv("RDS_MGMNT_TIMEOUT_OPERATIONS_GET", "3s")
		t.Setenv("RDS_MGMNT_TIMEOUT_ADAPTIVE_WINDOW_SIZE", "200")

		cfg, err := Load()
		require.NoError(t, err)

		assert.Equal(t, 10*time.Second, cfg.Timeout.BaseTimeout)
		assert.Equal(t, 3*time.Second, cfg.Timeout.Operations.Get)
		assert.Equal(t, 200, cfg.Timeout.Adaptive.WindowSize)
	})
}
