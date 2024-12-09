package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigLoad(t *testing.T) {
	t.Parallel()
	t.Run("Load Default Config", func(t *testing.T) {
		cfg, err := Load()
		require.NoError(t, err)
		assert.NotNil(t, cfg)
		assert.Equal(t, "localhost", cfg.Redis.Host)
		assert.Equal(t, "6379", cfg.Redis.Port)
	})

	t.Run("Load From Environment", func(t *testing.T) {
		os.Setenv("RDS_MGMNT_REDIS_HOST", "envhost")
		os.Setenv("RDS_MGMNT_REDIS_PORT", "6381")
		defer func() {
			os.Unsetenv("RDS_MGMNT_REDIS_HOST")
			os.Unsetenv("RDS_MGMNT_REDIS_PORT")
		}()

		cfg, err := Load()
		require.NoError(t, err)
		assert.Equal(t, "envhost", cfg.Redis.Host)
		assert.Equal(t, "6381", cfg.Redis.Port)
	})
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()

	// Load default config
	defaultCfg, err := Load()
	require.NoError(t, err)

	// Helper function to create modified config
	modifyConfig := func(modifiers ...func(*Config)) *Config {
		cfg := *defaultCfg // Create a copy of default config
		for _, modifier := range modifiers {
			modifier(&cfg)
		}
		return &cfg
	}

	tests := []struct {
		name        string
		modifiers   []func(*Config)
		expectError bool
	}{
		{
			name: "Valid Config",
			modifiers: []func(*Config){
				func(c *Config) {
					c.Redis = RedisConfig{
						Host:            "localhost",
						Port:            "6379",
						DB:              "0",
						Timeout:         5,
						RetryAttempts:   3,
						RetryDelay:      time.Second,
						MaxRetryBackoff: 5 * time.Second,
						ShutdownTimeout: 30,
					}
					c.Pool = PoolConfig{
						Status:      true,
						Size:        50,
						MinIdle:     10,
						MaxIdleTime: 300,
						WaitTimeout: 30,
					}
					c.Logging = LoggingConfig{
						Level:  "info",
						Format: "json",
						Output: "stdout",
					}
				},
			},
			expectError: false,
		},
		{
			name: "Missing Redis Host",
			modifiers: []func(*Config){
				func(c *Config) {
					c.Redis.Host = ""
					c.Redis.Port = "6379"
				},
			},
			expectError: true,
		},
		{
			name: "Invalid Pool Size",
			modifiers: []func(*Config){
				func(c *Config) {
					c.Redis.Host = "localhost"
					c.Redis.Port = "6379"
					c.Pool.Status = true
					c.Pool.Size = -1
				},
			},
			expectError: true,
		},
		{
			name: "Invalid Log Level",
			modifiers: []func(*Config){
				func(c *Config) {
					c.Redis.Host = "localhost"
					c.Redis.Port = "6379"
					c.Logging.Level = "invalid"
					c.Logging.Format = "json"
					c.Logging.Output = "stdout"
				},
			},
			expectError: true,
		},
		{
			name: "Missing File Path for File Output",
			modifiers: []func(*Config){
				func(c *Config) {
					c.Redis.Host = "localhost"
					c.Redis.Port = "6379"
					c.Logging.Level = "info"
					c.Logging.Format = "json"
					c.Logging.Output = "file"
					c.Logging.FilePath = ""
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable for parallel testing
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := modifyConfig(tt.modifiers...)
			err := ValidateConfig(cfg)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCircuitBreakerValidation(t *testing.T) {
	// Load default config
	defaultCfg, err := Load()
	require.NoError(t, err)

	// Helper function to create modified config
	modifyConfig := func(modifiers ...func(*CircuitConfig)) CircuitConfig {
		cfg := defaultCfg.Circuit // Create a copy of default circuit config
		for _, modifier := range modifiers {
			modifier(&cfg)
		}
		return cfg
	}

	tests := []struct {
		name        string
		modifiers   []func(*CircuitConfig)
		expectError bool
	}{
		{
			name: "Valid Circuit Config",
			modifiers: []func(*CircuitConfig){
				func(c *CircuitConfig) {
					c.Status = true
					c.Threshold = 5
					c.ResetTimeout = 10
					c.MaxHalfOpen = 2
				},
			},
			expectError: false,
		},
		{
			name: "Invalid Threshold",
			modifiers: []func(*CircuitConfig){
				func(c *CircuitConfig) {
					c.Status = true
					c.Threshold = 0
				},
			},
			expectError: true,
		},
		{
			name: "Disabled Circuit Breaker",
			modifiers: []func(*CircuitConfig){
				func(c *CircuitConfig) {
					c.Status = false
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := modifyConfig(tt.modifiers...)
			err := validateCircuitConfig(&cfg)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBulkOperationsValidation(t *testing.T) {
	t.Parallel()

	// Load default config
	defaultCfg, err := Load()
	require.NoError(t, err)

	// Helper function to create modified config
	modifyConfig := func(modifiers ...func(*BulkConfig)) BulkConfig {
		cfg := defaultCfg.Bulk // Create a copy of default bulk config
		for _, modifier := range modifiers {
			modifier(&cfg)
		}
		return cfg
	}

	tests := []struct {
		name        string
		modifiers   []func(*BulkConfig)
		expectError bool
	}{
		{
			name: "Valid Bulk Config",
			modifiers: []func(*BulkConfig){
				func(c *BulkConfig) {
					c.Status = true
					c.BatchSize = 1000
					c.FlushInterval = 5
					c.MaxRetries = 3
					c.ConcurrentFlush = true
				},
			},
			expectError: false,
		},
		{
			name: "Invalid Batch Size",
			modifiers: []func(*BulkConfig){
				func(c *BulkConfig) {
					c.Status = true
					c.BatchSize = 0
				},
			},
			expectError: true,
		},
		{
			name: "Disabled Bulk Operations",
			modifiers: []func(*BulkConfig){
				func(c *BulkConfig) {
					c.Status = false
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable for parallel testing
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := modifyConfig(tt.modifiers...)
			err := validateBulkConfig(&cfg)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
