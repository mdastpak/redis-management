package config

import (
	"fmt"
	"time"
)

func validateTimeoutConfig(c *TimeoutConfig) error {
	// Validate base settings
	if c.BaseTimeout <= 0 {
		return fmt.Errorf("base_timeout must be positive")
	}
	if c.MaxTimeout < c.BaseTimeout {
		return fmt.Errorf("max_timeout must be greater than or equal to base_timeout")
	}
	if c.MinTimeout <= 0 {
		return fmt.Errorf("min_timeout must be positive")
	}
	if c.MinTimeout > c.BaseTimeout {
		return fmt.Errorf("min_timeout must be less than or equal to base_timeout")
	}
	if c.BackoffFactor <= 1.0 {
		return fmt.Errorf("backoff_factor must be greater than 1.0")
	}

	// Validate operation timeouts
	if err := validateOperationTimeouts(&c.Operations, c.BaseTimeout); err != nil {
		return fmt.Errorf("operation timeouts: %w", err)
	}

	// Validate adaptive settings
	if c.Adaptive.Enabled {
		if err := validateAdaptiveConfig(&c.Adaptive); err != nil {
			return fmt.Errorf("adaptive config: %w", err)
		}
	}

	return nil
}

func validateOperationTimeouts(c *OperationTimeoutConfig, baseTimeout time.Duration) error {
	// Validate individual operation timeouts
	if c.Get <= 0 {
		return fmt.Errorf("get timeout must be positive")
	}
	if c.Set <= 0 {
		return fmt.Errorf("set timeout must be positive")
	}
	if c.Delete <= 0 {
		return fmt.Errorf("delete timeout must be positive")
	}
	if c.BulkBase <= 0 {
		return fmt.Errorf("bulk_base timeout must be positive")
	}

	// Validate relationships between timeouts
	maxOpTimeout := max(c.Get, max(c.Set, c.Delete))
	if maxOpTimeout > baseTimeout {
		return fmt.Errorf("operation timeouts must not exceed base_timeout")
	}

	return nil
}

func validateAdaptiveConfig(c *AdaptiveTimeoutConfig) error {
	if c.WindowSize <= 0 {
		return fmt.Errorf("window_size must be positive")
	}
	if c.AdjustmentThreshold <= 0 || c.AdjustmentThreshold >= 1 {
		return fmt.Errorf("adjustment_threshold must be between 0 and 1")
	}
	if c.MaxAdjustment <= 0 || c.MaxAdjustment >= 1 {
		return fmt.Errorf("max_adjustment must be between 0 and 1")
	}
	if c.HistoryRetention <= 0 {
		return fmt.Errorf("history_retention must be positive")
	}

	return nil
}

// Helper function for finding maximum duration
func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
