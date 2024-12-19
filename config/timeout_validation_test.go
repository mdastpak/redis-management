package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeoutConfigValidation(t *testing.T) {
	t.Parallel()

	t.Run("Basic Validation", func(t *testing.T) {
		tests := []struct {
			name        string
			modifyFunc  func(*TimeoutConfig)
			expectError string
		}{
			{
				name: "Invalid Base Timeout",
				modifyFunc: func(c *TimeoutConfig) {
					c.BaseTimeout = -1
				},
				expectError: "base_timeout must be positive",
			},
			{
				name: "Max Less Than Base",
				modifyFunc: func(c *TimeoutConfig) {
					c.MaxTimeout = c.BaseTimeout - time.Second
				},
				expectError: "max_timeout must be greater than or equal to base_timeout",
			},
			{
				name: "Min Greater Than Base",
				modifyFunc: func(c *TimeoutConfig) {
					c.MinTimeout = c.BaseTimeout + time.Second
				},
				expectError: "min_timeout must be less than or equal to base_timeout",
			},
			{
				name: "Invalid Backoff Factor",
				modifyFunc: func(c *TimeoutConfig) {
					c.BackoffFactor = 0.5
				},
				expectError: "backoff_factor must be greater than 1.0",
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

	t.Run("Operation Timeout Validation", func(t *testing.T) {
		tests := []struct {
			name        string
			modifyFunc  func(*TimeoutConfig)
			expectError string
		}{
			{
				name: "Invalid Get Timeout",
				modifyFunc: func(c *TimeoutConfig) {
					c.Operations.Get = -1
				},
				expectError: "get timeout must be positive",
			},
			{
				name: "Invalid Set Timeout",
				modifyFunc: func(c *TimeoutConfig) {
					c.Operations.Set = -1
				},
				expectError: "set timeout must be positive",
			},
			{
				name: "Invalid Delete Timeout",
				modifyFunc: func(c *TimeoutConfig) {
					c.Operations.Delete = -1
				},
				expectError: "delete timeout must be positive",
			},
			{
				name: "Operation Exceeds Base",
				modifyFunc: func(c *TimeoutConfig) {
					c.Operations.Get = c.BaseTimeout + time.Second
				},
				expectError: "operation timeouts must not exceed base_timeout",
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
}
