package config

import "time"

// Config represents the main configuration structure
type Config struct {
	Redis   RedisConfig   `mapstructure:"redis"`
	Pool    PoolConfig    `mapstructure:"pool"`
	Circuit CircuitConfig `mapstructure:"circuit"`
	Logging LoggingConfig `mapstructure:"logging"`
	Timeout TimeoutConfig `mapstructure:"timeout"`
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
	ShutdownTimeout     time.Duration `mapstructure:"shutdown_timeout"`
}

// PoolConfig holds connection pool configuration
type PoolConfig struct {
	Status      bool `mapstructure:"status"`
	Size        int  `mapstructure:"size"`
	MinIdle     int  `mapstructure:"min_idle"`
	MaxIdleTime int  `mapstructure:"max_idle_time"`
	WaitTimeout int  `mapstructure:"wait_timeout"`
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
