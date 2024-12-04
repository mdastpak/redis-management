// config.go
package config

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
)

// Config represents the main configuration structure

type Config struct {
	Redis   RedisConfig   `mapstructure:"redis"`
	Pool    PoolConfig    `mapstructure:"pool"`
	Bulk    BulkConfig    `mapstructure:"bulk"`
	Circuit CircuitConfig `mapstructure:"circuit"`
	Logging LoggingConfig `mapstructure:"logging"`
}

// RedisConfig holds Redis-specific configuration
type RedisConfig struct {
	Host                string        `mapstructure:"host"`
	Port                string        `mapstructure:"port"`
	Password            string        `mapstructure:"password"`
	DB                  string        `mapstructure:"db"`
	KeyPrefix           string        `mapstructure:"key_prefix"`
	TTL                 time.Duration `mapstructure:"ttl"`
	Timeout             int           `mapstructure:"timeout"`
	HashKeys            bool          `mapstructure:"hash_keys"`
	HealthCheckInterval int           `mapstructure:"health_check_interval"`
	RetryAttempts       int           `mapstructure:"retry_attempts"`
	RetryDelay          time.Duration `mapstructure:"retry_delay"`
	MaxRetryBackoff     time.Duration `mapstructure:"max_retry_backoff"`
}

// PoolConfig holds connection pool configuration
type PoolConfig struct {
	Status      bool `mapstructure:"status"`
	Size        int  `mapstructure:"size"`
	MinIdle     int  `mapstructure:"min_idle"`
	MaxIdleTime int  `mapstructure:"max_idle_time"`
	WaitTimeout int  `mapstructure:"wait_timeout"`
}

// BulkConfig holds bulk operation configuration
type BulkConfig struct {
	Status          bool `mapstructure:"status"`
	BatchSize       int  `mapstructure:"batch_size"`
	FlushInterval   int  `mapstructure:"flush_interval"`
	MaxRetries      int  `mapstructure:"max_retries"`
	ConcurrentFlush bool `mapstructure:"concurrent_flush"`
}

type CircuitConfig struct {
	Status       bool  `mapstructure:"status"`        // Enable/disable circuit breaker
	Threshold    int64 `mapstructure:"threshold"`     // Number of failures before opening
	ResetTimeout int   `mapstructure:"reset_timeout"` // Time in seconds to wait before reset
	MaxHalfOpen  int32 `mapstructure:"max_half_open"` // Max concurrent requests in half-open state
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level    string `mapstructure:"level"`
	Format   string `mapstructure:"format"`
	Output   string `mapstructure:"output"`
	FilePath string `mapstructure:"file_path"`
}

// LoadConfig loads configuration from file and environment variables

func Load(path string) (*Config, error) {
	viper.SetConfigFile(path)

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %v", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %v", err)
	}

	return &cfg, nil
}

func LoadConfig(configPath string) (*Config, error) {
	var config Config

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	if configPath != "" {
		// Use config file from the given path
		viper.AddConfigPath(filepath.Dir(configPath))
		viper.SetConfigName(filepath.Base(configPath))
	} else {
		// Search config in default locations
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("$HOME/.redis-management")
	}

	// Read environment variables prefixed with REDIS_
	viper.SetEnvPrefix("REDIS")
	viper.AutomaticEnv()

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return nil, fmt.Errorf("config file not found: %v", err)
		}
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	// Unmarshal config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %v", err)
	}

	// Validate config
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %v", err)
	}

	return &config, nil
}

// validateConfig performs validation on the configuration values
func validateConfig(config *Config) error {
	// Validate Redis config
	if config.Redis.Host == "" {
		return fmt.Errorf("redis host cannot be empty")
	}
	if config.Redis.Port == "" {
		return fmt.Errorf("redis port cannot be empty")
	}
	if config.Redis.Timeout <= 0 {
		return fmt.Errorf("redis timeout must be positive")
	}

	// Validate Pool config if enabled
	if config.Pool.Status {
		if config.Pool.Size <= 0 {
			return fmt.Errorf("pool size must be positive")
		}
		if config.Pool.MinIdle < 0 {
			return fmt.Errorf("min idle connections cannot be negative")
		}
		if config.Pool.MinIdle > config.Pool.Size {
			return fmt.Errorf("min idle connections cannot be greater than pool size")
		}
	}

	// Validate Bulk config if enabled
	if config.Bulk.Status {
		if config.Bulk.BatchSize <= 0 {
			return fmt.Errorf("batch size must be positive")
		}
		if config.Bulk.FlushInterval <= 0 {
			return fmt.Errorf("flush interval must be positive")
		}
	}

	return nil
}
