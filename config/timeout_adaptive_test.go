package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeoutAdaptiveConfig(t *testing.T) {
	t.Parallel()

	t.Run("Adaptive Configuration Validation", func(t *testing.T) {
		tests := []struct {
			name        string
			modifyFunc  func(*TimeoutConfig)
			expectError string
		}{
			{
				name: "Invalid Window Size",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.WindowSize = 0
				},
				expectError: "window_size must be positive",
			},
			{
				name: "Invalid Adjustment Threshold Low",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.AdjustmentThreshold = -0.1
				},
				expectError: "adjustment_threshold must be between 0 and 1",
			},
			{
				name: "Invalid Adjustment Threshold High",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.AdjustmentThreshold = 1.5
				},
				expectError: "adjustment_threshold must be between 0 and 1",
			},
			{
				name: "Invalid Max Adjustment",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.MaxAdjustment = 2.0
				},
				expectError: "max_adjustment must be between 0 and 1",
			},
			{
				name: "Invalid History Retention",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.HistoryRetention = -1
				},
				expectError: "history_retention must be positive",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg, err := Load()
				require.NoError(t, err)

				tt.modifyFunc(&cfg.Timeout)
				err = ValidateConfig(cfg)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			})
		}
	})

	t.Run("Adaptive Settings When Disabled", func(t *testing.T) {
		t.Parallel()
		cfg, err := Load()
		require.NoError(t, err)

		// Set invalid values but disable adaptive settings
		cfg.Timeout.Adaptive.Enabled = false
		cfg.Timeout.Adaptive.WindowSize = 0
		cfg.Timeout.Adaptive.AdjustmentThreshold = 2.0
		cfg.Timeout.Adaptive.MaxAdjustment = -1
		cfg.Timeout.Adaptive.HistoryRetention = -1

		// Should not fail validation when disabled
		err = ValidateConfig(cfg)
		assert.NoError(t, err)
	})

	t.Run("Valid Adaptive Configurations", func(t *testing.T) {
		tests := []struct {
			name       string
			modifyFunc func(*TimeoutConfig)
		}{
			{
				name: "Small Window Size",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.WindowSize = 10
				},
			},
			{
				name: "Large Window Size",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.WindowSize = 1000
				},
			},
			{
				name: "Small Adjustments",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.AdjustmentThreshold = 0.05
					c.Adaptive.MaxAdjustment = 0.1
				},
			},
			{
				name: "Long History Retention",
				modifyFunc: func(c *TimeoutConfig) {
					c.Adaptive.Enabled = true
					c.Adaptive.HistoryRetention = 7 * 24 * time.Hour
				},
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				cfg, err := Load()
				require.NoError(t, err)

				tt.modifyFunc(&cfg.Timeout)
				err = ValidateConfig(cfg)
				assert.NoError(t, err)
			})
		}
	})
}
