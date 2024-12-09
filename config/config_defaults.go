package config

import (
	"time"

	"github.com/spf13/viper"
)

func setDefaults(v *viper.Viper) {
	// Redis defaults
	v.SetDefault("redis.host", "localhost")
	v.SetDefault("redis.port", "6379")
	v.SetDefault("redis.db", "0")                              // Can be single number or range (e.g., "0" or "0-5")
	v.SetDefault("redis.key_prefix", "")                       // Optional prefix for all keys
	v.SetDefault("redis.ttl", time.Second*60)                  //Default TTL for keys in seconds
	v.SetDefault("redis.timeout", time.Second*5)               //Connection timeout in seconds
	v.SetDefault("redis.hash_keys", true)                      //Whether to hash keys using SHA-256
	v.SetDefault("redis.health_check_interval", time.Second*1) //Health check interval in seconds
	v.SetDefault("redis.retry_attempts", 3)                    //Number of retry attempts for failed operations
	v.SetDefault("redis.retry_delay", 1000)                    // Delay between retry attempts in milliseconds
	v.SetDefault("redis.max_retry_backoff", 5000)              // Maximum backoff time in milliseconds
	v.SetDefault("redis.shutdown_timeout", time.Second*30)     // Timeout for graceful shutdown in seconds

	// Pool defaults
	v.SetDefault("pool.status", false)                // Enable/disable connection pooling
	v.SetDefault("pool.size", 10)                     // Maximum number of connections in the pool
	v.SetDefault("pool.min_idle", 5)                  // Minimum number of idle connections in the pool
	v.SetDefault("pool.max_idle_time", 300)           // Maximum time a connection can be idle in seconds
	v.SetDefault("pool.wait_timeout", time.Second*30) // Timeout for waiting for a connection in seconds

	// Bulk defaults
	v.SetDefault("bulk.status", false)          // Enable/disable bulk operations
	v.SetDefault("bulk.batch_size", 10)         // Maximum number of operations in a batch
	v.SetDefault("bulk.flush_interval", 1)      // Maximum time between flushes in seconds
	v.SetDefault("bulk.max_retries", 1)         // Maximum number of retries for failed bulk operations
	v.SetDefault("bulk.concurrent_flush", true) // Whether to flush in a separate goroutine

	// Circuit defaults
	v.SetDefault("circuit.status", false)                 // Enable/disable circuit breaker
	v.SetDefault("circuit.threshold", 5)                  // Number of consecutive failures to trip the circuit
	v.SetDefault("circuit.reset_timeout", time.Second*10) // Time to wait before resetting the circuit in seconds
	v.SetDefault("circuit.max_half_open", 2)              // Maximum number of half-open attempts before resetting the circuit

	// Logging defaults
	v.SetDefault("logging.level", "info")    // Log level (trace, debug, info, warn, error, fatal, panic)
	v.SetDefault("logging.format", "json")   // Log format (json, text)
	v.SetDefault("logging.output", "stdout") // Log output (stdout, stderr, file)
}
