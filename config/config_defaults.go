package config

import (
	"time"

	"github.com/spf13/viper"
)

func setDefaults(v *viper.Viper) {
	// Redis defaults
	v.SetDefault("redis.host", "localhost")
	v.SetDefault("redis.port", "6379")
	v.SetDefault("redis.db", "0")                                // Can be single number or range (e.g., "0" or "0-5")
	v.SetDefault("redis.key_prefix", "")                         // Optional prefix for all keys
	v.SetDefault("redis.ttl", time.Second*60)                    //Default TTL for keys in seconds
	v.SetDefault("redis.timeout", time.Second*5)                 //Connection timeout in seconds
	v.SetDefault("redis.hash_keys", true)                        //Whether to hash keys using SHA-256
	v.SetDefault("redis.health_check_interval", time.Second*1)   //Health check interval in seconds
	v.SetDefault("redis.retry_attempts", 3)                      //Number of retry attempts for failed operations
	v.SetDefault("redis.retry_delay", 1000)                      // Delay between retry attempts in milliseconds
	v.SetDefault("redis.max_retry_backoff", 5000)                // Maximum backoff time in milliseconds
	v.SetDefault("redis.shutdown_timeout", time.Millisecond*100) // Timeout for graceful shutdown in seconds

	// Pool defaults
	v.SetDefault("pool.status", false)                // Enable/disable connection pooling
	v.SetDefault("pool.size", 10)                     // Maximum number of connections in the pool
	v.SetDefault("pool.min_idle", 5)                  // Minimum number of idle connections in the pool
	v.SetDefault("pool.max_idle_time", 300)           // Maximum time a connection can be idle in seconds
	v.SetDefault("pool.wait_timeout", time.Second*30) // Timeout for waiting for a connection in seconds

	// Circuit defaults
	v.SetDefault("circuit.status", false)                 // Enable/disable circuit breaker
	v.SetDefault("circuit.threshold", 5)                  // Number of consecutive failures to trip the circuit
	v.SetDefault("circuit.reset_timeout", time.Second*10) // Time to wait before resetting the circuit in seconds
	v.SetDefault("circuit.max_half_open", 2)              // Maximum number of half-open attempts before resetting the circuit

	// Logging defaults
	v.SetDefault("logging.level", "info")    // Log level (trace, debug, info, warn, error, fatal, panic)
	v.SetDefault("logging.format", "json")   // Log format (json, text)
	v.SetDefault("logging.output", "stdout") // Log output (stdout, stderr, file)

	// Timeout management defaults
	v.SetDefault("timeout.base_timeout", time.Second*5)
	v.SetDefault("timeout.max_timeout", time.Second*30)
	v.SetDefault("timeout.min_timeout", time.Millisecond*100)
	v.SetDefault("timeout.backoff_factor", 1.5)

	// Operation timeout defaults
	v.SetDefault("timeout.operations.get", time.Second*1)
	v.SetDefault("timeout.operations.set", time.Second*2)
	v.SetDefault("timeout.operations.delete", time.Second*2)
	v.SetDefault("timeout.operations.bulk_base", time.Second*5)

	// Adaptive timeout defaults
	v.SetDefault("timeout.adaptive.enabled", true)
	v.SetDefault("timeout.adaptive.window_size", 100)
	v.SetDefault("timeout.adaptive.adjustment_threshold", 0.2) // 20%
	v.SetDefault("timeout.adaptive.max_adjustment", 0.5)       // 50%
	v.SetDefault("timeout.adaptive.history_retention", time.Hour*24)
}
