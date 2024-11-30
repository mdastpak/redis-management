// pkg/management/pool.go
package management

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// initPool initializes the connection pool
func (rs *RedisService) initPool() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	options := &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Port),
		Password:     rs.cfg.Redis.Password,
		DialTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		ReadTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		WriteTimeout: time.Duration(rs.cfg.Redis.Timeout) * time.Second,

		// Pool specific configurations
		PoolSize:     rs.cfg.Pool.Size,
		MinIdleConns: rs.cfg.Pool.MinIdle,
		MaxConnAge:   time.Duration(rs.cfg.Pool.MaxIdleTime) * time.Second,
		PoolTimeout:  time.Duration(rs.cfg.Pool.WaitTimeout) * time.Second,

		// OnConnect handler to ensure connection is valid
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			return cn.Ping(ctx).Err()
		},
	}

	rs.pool = redis.NewClient(options)

	// Test the pool connection
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rs.cfg.Redis.Timeout)*time.Second)
	defer cancel()

	if err := rs.pool.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis pool: %v", err)
	}

	// Start health check if enabled
	if rs.cfg.Redis.HealthCheckInterval > 0 {
		go rs.startPoolHealthCheck()
	}

	return nil
}

// startPoolHealthCheck performs periodic health checks on the pool
func (rs *RedisService) startPoolHealthCheck() {
	ticker := time.NewTicker(time.Duration(rs.cfg.Redis.HealthCheckInterval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rs.mu.RLock()
		if rs.pool == nil {
			rs.mu.RUnlock()
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rs.cfg.Redis.Timeout)*time.Second)
		err := rs.pool.Ping(ctx).Err()
		cancel()

		if err != nil {
			// Log error and attempt to reinitialize the pool
			fmt.Printf("Pool health check failed: %v\n", err)
			rs.mu.RUnlock()

			// Attempt to reinitialize the pool
			if err := rs.initPool(); err != nil {
				fmt.Printf("Failed to reinitialize pool: %v\n", err)
			}
		} else {
			rs.mu.RUnlock()
		}
	}
}

// // getPoolStats returns current pool statistics
// func (rs *RedisService) getPoolStats() *redis.PoolStats {
// 	rs.mu.RLock()
// 	defer rs.mu.RUnlock()

// 	if rs.pool != nil {
// 		return rs.pool.PoolStats()
// 	}
// 	return nil
// }

func (rs *RedisService) getPoolStats() *redis.PoolStats {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if rs.pool == nil {
		return nil
	}

	return rs.pool.PoolStats()
}
