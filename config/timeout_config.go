package config

import "time"

// TimeoutConfig holds configuration for timeout management
type TimeoutConfig struct {
	// Base timeout duration for operations
	BaseTimeout time.Duration `mapstructure:"base_timeout"`

	// Maximum timeout duration allowed
	MaxTimeout time.Duration `mapstructure:"max_timeout"`

	// Minimum timeout duration allowed
	MinTimeout time.Duration `mapstructure:"min_timeout"`

	// Factor for exponential backoff
	BackoffFactor float64 `mapstructure:"backoff_factor"`

	// Timeout settings for different operation types
	Operations OperationTimeoutConfig `mapstructure:"operations"`

	// Adaptive timeout settings
	Adaptive AdaptiveTimeoutConfig `mapstructure:"adaptive"`
}

// OperationTimeoutConfig holds timeout configuration for specific operations
type OperationTimeoutConfig struct {
	// Get operation timeout
	Get time.Duration `mapstructure:"get"`

	// Set operation timeout
	Set time.Duration `mapstructure:"set"`

	// Delete operation timeout
	Delete time.Duration `mapstructure:"delete"`

	// Bulk operation timeout base (per item)
	BulkBase time.Duration `mapstructure:"bulk_base"`
}

// AdaptiveTimeoutConfig holds configuration for adaptive timeout behavior
type AdaptiveTimeoutConfig struct {
	// Enable adaptive timeout adjustment
	Enabled bool `mapstructure:"enabled"`

	// Window size for calculating average operation time
	WindowSize int `mapstructure:"window_size"`

	// Adjustment threshold percentage
	AdjustmentThreshold float64 `mapstructure:"adjustment_threshold"`

	// Maximum adjustment percentage per update
	MaxAdjustment float64 `mapstructure:"max_adjustment"`

	// History retention period
	HistoryRetention time.Duration `mapstructure:"history_retention"`
}
