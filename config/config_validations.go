package config

import (
	"fmt"
	"reflect"
)

// ValidateConfig performs validation on the configuration values
func ValidateConfig(config *Config) error {
	// Redis validations
	if err := validateRedisConfig(&config.Redis); err != nil {
		return fmt.Errorf("redis config: %w", err)
	}

	// Pool validations
	if err := validatePoolConfig(&config.Pool); err != nil {
		return fmt.Errorf("pool config: %w", err)
	}

	// Bulk validations
	if err := validateBulkConfig(&config.Bulk); err != nil {
		return fmt.Errorf("bulk config: %w", err)
	}

	// Circuit validations
	if err := validateCircuitConfig(&config.Circuit); err != nil {
		return fmt.Errorf("circuit config: %w", err)
	}

	// Logging validations
	if err := validateLoggingConfig(&config.Logging); err != nil {
		return fmt.Errorf("logging config: %w", err)
	}

	return nil
}

func validateRedisConfig(c *RedisConfig) error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.Port == "" {
		return fmt.Errorf("port is required")
	}
	if c.DB == "" {
		return fmt.Errorf("db is required")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if c.TTL < 0 {
		return fmt.Errorf("ttl cannot be negative")
	}
	if c.RetryAttempts < 0 {
		return fmt.Errorf("retry attempts cannot be negative")
	}
	if c.RetryDelay <= 0 {
		return fmt.Errorf("retry delay must be positive")
	}
	if c.MaxRetryBackoff <= 0 {
		return fmt.Errorf("max retry backoff must be positive")
	}
	if c.MaxRetryBackoff < c.RetryDelay {
		return fmt.Errorf("max retry backoff must be greater than retry delay")
	}
	if c.HealthCheckInterval < 0 {
		return fmt.Errorf("health check interval cannot be negative")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown timeout must be positive")
	}
	return nil
}

func validatePoolConfig(c *PoolConfig) error {
	if reflect.TypeOf(c.Status).Kind() != reflect.Bool {
		return fmt.Errorf("status must be boolean, got %T", c.Status)
	}
	if c.Size <= 0 {
		return fmt.Errorf("size must be positive")
	}
	if c.MinIdle < 0 {
		return fmt.Errorf("min idle cannot be negative")
	}
	if c.MinIdle > c.Size {
		return fmt.Errorf("min idle cannot be greater than size")
	}
	if c.MaxIdleTime <= 0 {
		return fmt.Errorf("max idle time must be positive")
	}
	if c.WaitTimeout <= 0 {
		return fmt.Errorf("wait timeout must be positive")
	}
	return nil
}

func validateBulkConfig(c *BulkConfig) error {
	if reflect.TypeOf(c.Status).Kind() != reflect.Bool {
		return fmt.Errorf("status must be boolean, got %T", c.Status)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	if c.FlushInterval <= 0 {
		return fmt.Errorf("flush interval must be positive")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	return nil
}

func validateCircuitConfig(c *CircuitConfig) error {
	if reflect.TypeOf(c.Status).Kind() != reflect.Bool {
		return fmt.Errorf("status must be boolean, got %T", c.Status)
	}
	if c.Threshold <= 0 {
		return fmt.Errorf("threshold must be positive")
	}
	if c.ResetTimeout <= 0 {
		return fmt.Errorf("reset timeout must be positive")
	}
	if c.MaxHalfOpen <= 0 {
		return fmt.Errorf("max half open must be positive")
	}
	return nil
}

func validateLoggingConfig(c *LoggingConfig) error {
	if c.Level == "" {
		return fmt.Errorf("level is required")
	}
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLevels[c.Level] {
		return fmt.Errorf("invalid log level: %s", c.Level)
	}

	if c.Format == "" {
		return fmt.Errorf("format is required")
	}
	validFormats := map[string]bool{
		"json": true,
		"text": true,
	}
	if !validFormats[c.Format] {
		return fmt.Errorf("invalid log format: %s", c.Format)
	}

	if c.Output == "" {
		return fmt.Errorf("output is required")
	}
	validOutputs := map[string]bool{
		"stdout": true,
		"file":   true,
	}
	if !validOutputs[c.Output] {
		return fmt.Errorf("invalid log output: %s", c.Output)
	}

	if c.Output == "file" && c.FilePath == "" {
		return fmt.Errorf("file path is required when output is set to file")
	}
	return nil
}
