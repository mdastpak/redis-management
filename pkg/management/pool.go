// pkg/management/pool.go

package management

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// Pool dynamic management constants
const (
	scaleUpThreshold   = 0.8              // 80% pool utilization triggers scale up
	scaleDownThreshold = 0.3              // 30% pool utilization triggers scale down
	scaleUpFactor      = 1.2              // Increase pool size by 20%
	scaleDownFactor    = 0.8              // Decrease pool size by 20%
	minScaleInterval   = 30 * time.Second // Minimum time between scaling operations
)

// Dynamic pool adjustment
func (rs *RedisService) adjustPoolSize() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.pool == nil {
		return
	}

	stats := rs.getPoolStats()
	if stats == nil {
		return
	}

	currentSize := rs.cfg.Pool.Size
	utilization := float64(stats.TotalConns) / float64(currentSize)

	// Log current state
	log.Printf("Pool utilization: %.2f%% (Total: %d, Size: %d)",
		utilization*100, stats.TotalConns, currentSize)

	// Scale up if needed
	if utilization >= scaleUpThreshold {
		newSize := int(float64(currentSize) * scaleUpFactor)
		if newSize > currentSize {
			log.Printf("Scaling up pool from %d to %d connections", currentSize, newSize)
			if err := rs.resizePool(newSize); err != nil {
				log.Printf("Failed to scale up pool: %v", err)
				return
			}
			rs.cfg.Pool.Size = newSize
		}
		return
	}

	// Scale down if possible
	if utilization <= scaleDownThreshold {
		newSize := int(float64(currentSize) * scaleDownFactor)
		if newSize >= rs.cfg.Pool.MinIdle {
			log.Printf("Scaling down pool from %d to %d connections", currentSize, newSize)
			if err := rs.resizePool(newSize); err != nil {
				log.Printf("Failed to scale down pool: %v", err)
				return
			}
			rs.cfg.Pool.Size = newSize
		}
	}
}

// resizePool handles the actual resizing operation
func (rs *RedisService) resizePool(newSize int) error {
	// Create new options with updated pool size
	options := &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Port),
		Password:     rs.cfg.Redis.Password,
		DialTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		ReadTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		WriteTimeout: time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		PoolSize:     newSize,
		MinIdleConns: rs.cfg.Pool.MinIdle,
		MaxConnAge:   time.Duration(rs.cfg.Pool.MaxIdleTime) * time.Second,
		PoolTimeout:  time.Duration(rs.cfg.Pool.WaitTimeout) * time.Second,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			return cn.Ping(ctx).Err()
		},
	}

	// Create new client with updated pool
	newPool := redis.NewClient(options)

	// Test new pool
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(rs.cfg.Redis.Timeout)*time.Second)
	defer cancel()

	if err := newPool.Ping(ctx).Err(); err != nil {
		newPool.Close()
		return fmt.Errorf("failed to initialize resized pool: %v", err)
	}

	// Close old pool and switch to new one
	if rs.pool != nil {
		oldPool := rs.pool
		rs.pool = newPool
		oldPool.Close()
	} else {
		rs.pool = newPool
	}

	return nil
}

// startPoolSizeMonitor starts the dynamic pool size monitoring
func (rs *RedisService) startPoolSizeMonitor() {
	ticker := time.NewTicker(minScaleInterval)
	defer ticker.Stop()

	for range ticker.C {
		rs.adjustPoolSize()
	}
}

// initPool initializes the connection pool with enhanced monitoring and include dynamic sizing
func (rs *RedisService) initPool() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	options := &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Port),
		Password:     rs.cfg.Redis.Password,
		DialTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		ReadTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		WriteTimeout: time.Duration(rs.cfg.Redis.Timeout) * time.Second,

		// Enhanced pool configurations
		PoolSize:     rs.cfg.Pool.Size,
		MinIdleConns: rs.cfg.Pool.MinIdle,
		MaxConnAge:   time.Duration(rs.cfg.Pool.MaxIdleTime) * time.Second,
		PoolTimeout:  time.Duration(rs.cfg.Pool.WaitTimeout) * time.Second,

		// Connection validation
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			return cn.Ping(ctx).Err()
		},
	}

	rs.pool = redis.NewClient(options)

	// Validate pool connection
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(rs.cfg.Redis.Timeout)*time.Second)
	defer cancel()

	if err := rs.pool.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis pool: %v", err)
	}

	// Initialize pool monitoring
	if rs.cfg.Redis.HealthCheckInterval > 0 {
		go rs.startPoolHealthCheck()
		go rs.startPoolSizeMonitor()
	}

	log.Printf("Pool initialized - Size: %d, MinIdle: %d, MaxIdleTime: %d",
		rs.cfg.Pool.Size, rs.cfg.Pool.MinIdle, rs.cfg.Pool.MaxIdleTime)

	return nil
}

// startPoolHealthCheck performs enhanced health monitoring
func (rs *RedisService) startPoolHealthCheck() {
	ticker := time.NewTicker(time.Duration(rs.cfg.Redis.HealthCheckInterval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rs.mu.RLock()
		if rs.pool == nil {
			rs.mu.RUnlock()
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(rs.cfg.Redis.Timeout)*time.Second)

		// Perform health check
		err := rs.pool.Ping(ctx).Err()
		stats := rs.getPoolStats()

		if err != nil {
			log.Printf("Pool health check failed: %v", err)
			if stats != nil {
				log.Printf("Pool stats before reinit - TotalConns: %d, IdleConns: %d",
					stats.TotalConns, stats.IdleConns)
			}
			rs.mu.RUnlock()
			cancel()

			if err := rs.initPool(); err != nil {
				log.Printf("Failed to reinitialize pool: %v", err)
			}
		} else {
			if stats != nil {
				log.Printf("Pool health check passed - TotalConns: %d, IdleConns: %d",
					stats.TotalConns, stats.IdleConns)
			}
			rs.mu.RUnlock()
			cancel()
		}
	}
}

// getPoolStats returns current pool statistics with additional safety checks
func (rs *RedisService) getPoolStats() *redis.PoolStats {
	// Note: Caller must hold at least a read lock
	if rs.pool == nil {
		return nil
	}
	return rs.pool.PoolStats()
}
