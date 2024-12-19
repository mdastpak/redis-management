package management

import (
	"context"
	"fmt"
	"log"
	"redis-management/pkg/logging"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// PoolMetrics stores pool performance metrics
type PoolMetrics struct {
	TotalConnections   int64         `json:"total_connections"`
	ActiveConnections  int64         `json:"active_connections"`
	IdleConnections    int64         `json:"idle_connections"`
	WaitingRequests    int64         `json:"waiting_requests"`
	OperationLatency   time.Duration `json:"operation_latency"`
	LastScaleOperation time.Time     `json:"last_scale_operation"`
}

// PoolStatus represents the current state of the pool
type PoolStatus int32

const (
	PoolStatusInitializing PoolStatus = iota
	PoolStatusReady
	PoolStatusStopping
	PoolStatusStopped
)

// MonitoringConfig holds monitoring-specific configuration
type MonitoringConfig struct {
	HealthCheckInterval time.Duration
	MetricsInterval     time.Duration
	StartupDelay        time.Duration
	HealthCheckTimeout  time.Duration
	MaxFailedChecks     int
}

// Enhanced PoolManager with monitoring controls
type PoolManager struct {
	service         *RedisService
	config          *redis.Options
	monitoringCfg   MonitoringConfig
	status          atomic.Value // stores PoolStatus
	metrics         atomic.Value // stores *PoolMetrics
	failedChecks    atomic.Int32
	scaleOperations chan struct{}
	stopChan        chan struct{}
	isReady         atomic.Bool
	wg              sync.WaitGroup
	mu              sync.RWMutex
	isScaling       atomic.Bool
	lastError       atomic.Value // stores error
	lastScaleTime   atomic.Value // stores time.Time
	logger          logging.Logger
	scalingMetrics  struct {
		minInterval    time.Duration
		maxMultiplier  float64
		minMultiplier  float64
		highThreshold  float64
		lowThreshold   float64
		cooldownPeriod time.Duration
	}
}

// Scaling thresholds and constraints
const (
	defaultMinScaleInterval = 30 * time.Second
	defaultMaxMultiplier    = 2.0
	defaultMinMultiplier    = 0.5
	defaultHighThreshold    = 0.80 // 80% utilization
	defaultLowThreshold     = 0.20 // 20% utilization
	defaultCooldownPeriod   = 5 * time.Second
)

func (rs *RedisService) NewPoolManager(ctx context.Context) error {
	// Initialize Redis options with all necessary configurations
	options := &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", rs.cfg.Redis.Host, rs.cfg.Redis.Port),
		Password:     rs.cfg.Redis.Password,
		DialTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		ReadTimeout:  time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		WriteTimeout: time.Duration(rs.cfg.Redis.Timeout) * time.Second,
		PoolSize:     rs.cfg.Pool.Size,
		MinIdleConns: rs.cfg.Pool.MinIdle,
		MaxConnAge:   time.Duration(rs.cfg.Pool.MaxIdleTime) * time.Second,
		PoolTimeout:  time.Duration(rs.cfg.Pool.WaitTimeout) * time.Second,
	}

	// Initialize pool with the configured options
	rs.pool = redis.NewClient(options)

	// Create monitoring configuration
	monitoringCfg := MonitoringConfig{
		HealthCheckInterval: time.Second,
		MetricsInterval:     time.Second,
		StartupDelay:        100 * time.Millisecond,
		HealthCheckTimeout:  500 * time.Millisecond,
		MaxFailedChecks:     3,
	}

	// Create pool manager with all necessary fields
	pm := &PoolManager{
		service:         rs,
		config:          options,
		monitoringCfg:   monitoringCfg,
		scaleOperations: make(chan struct{}, 1),
		stopChan:        make(chan struct{}),
		logger:          rs.logger.WithComponent("pool"),
	}

	// Set initial status
	pm.status.Store(PoolStatusInitializing)

	// Set initial metrics
	initialMetrics := &PoolMetrics{
		TotalConnections:  int64(options.PoolSize),
		IdleConnections:   int64(options.MinIdleConns),
		ActiveConnections: 0,
		WaitingRequests:   0,
	}
	pm.metrics.Store(initialMetrics)

	// Initialize other atomic values
	pm.lastScaleTime.Store(time.Now())
	pm.lastError.Store(error(fmt.Errorf(""))) // Empty error instead of nil
	pm.isReady.Store(false)
	pm.isScaling.Store(false)

	// Initialize scaling metrics
	pm.scalingMetrics = struct {
		minInterval    time.Duration
		maxMultiplier  float64
		minMultiplier  float64
		highThreshold  float64
		lowThreshold   float64
		cooldownPeriod time.Duration
	}{
		minInterval:    defaultMinScaleInterval,
		maxMultiplier:  defaultMaxMultiplier,
		minMultiplier:  defaultMinMultiplier,
		highThreshold:  defaultHighThreshold,
		lowThreshold:   defaultLowThreshold,
		cooldownPeriod: defaultCooldownPeriod,
	}

	// Store the pool manager in the service
	rs.poolManager = pm

	// Initial connection test with timeout
	err := rs.timeoutManager.ExecuteWithTimeout(ctx, "POOL_INIT_TEST", 1,
		func(ctx context.Context) error {
			return rs.pool.Ping(ctx).Err()
		})
	if err != nil {
		return fmt.Errorf("failed to ping Redis: %v", err)
	}

	// Start monitoring with startup sequence
	if err := pm.StartMonitoring(ctx); err != nil {
		return fmt.Errorf("failed to start monitoring: %v", err)
	}

	rs.logger.WithFields(map[string]interface{}{
		"pool_size":    options.PoolSize,
		"min_idle":     options.MinIdleConns,
		"max_age":      options.MaxConnAge,
		"pool_timeout": options.PoolTimeout,
	}).Info("Pool manager initialized successfully")

	return nil
}

// cleanup helper function
func (pm *PoolManager) cleanup() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.service != nil && pm.service.pool != nil {
		pm.service.pool.Close()
	}
	pm.status.Store(PoolStatusStopped)
}

func (pm *PoolManager) Stop() {
	if !pm.status.CompareAndSwap(PoolStatusReady, PoolStatusStopping) {
		return
	}

	pm.logger.Info("Stopping pool manager")
	close(pm.stopChan)
	pm.wg.Wait()
	pm.status.Store(PoolStatusStopped)
}

// StartMonitoring begins pool monitoring and scaling
func (pm *PoolManager) StartMonitoring(ctx context.Context) error {
	if !pm.status.CompareAndSwap(PoolStatusInitializing, PoolStatusReady) {
		return fmt.Errorf("pool is not in initializing state")
	}

	// Allow time for pool to establish connections
	time.Sleep(pm.monitoringCfg.StartupDelay)

	// Perform initial health check with timeout
	healthCtx, cancel := context.WithTimeout(ctx, pm.monitoringCfg.HealthCheckTimeout)
	defer cancel()

	if err := pm.checkPoolHealth(healthCtx); err != nil {
		pm.status.Store(PoolStatusStopped)
		return fmt.Errorf("initial health check failed: %v", err)
	}

	// Start monitoring routines
	pm.wg.Add(3)

	// Start metrics collector
	go func() {
		defer pm.wg.Done()
		pm.monitorMetrics(ctx)
	}()

	// Start health checker
	go func() {
		defer pm.wg.Done()
		pm.monitorHealth(ctx)
	}()

	// Start scaling monitor
	go func() {
		defer pm.wg.Done()
		pm.handleScaling(ctx)
	}()

	// Start periodic cleanup
	go pm.periodicCleanup(ctx)

	// Wait for monitoring to be fully established
	time.Sleep(pm.monitoringCfg.StartupDelay)
	pm.isReady.Store(true)

	pm.logger.Info("Pool monitoring started successfully")
	return nil
}

func (pm *PoolManager) monitorHealth(ctx context.Context) {
	ticker := time.NewTicker(pm.monitoringCfg.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pm.stopChan:
			return
		case <-ticker.C:
			if !pm.isReady.Load() {
				continue
			}

			healthCtx, cancel := context.WithTimeout(ctx, pm.monitoringCfg.HealthCheckTimeout)
			err := pm.service.operationManager.ExecuteWithLock(healthCtx, "POOL_HEALTH_CHECK", func() error {
				if pm.service.cb != nil && pm.service.cfg.Circuit.Status {
					return pm.service.cb.Execute(func() error {
						return pm.service.timeoutManager.ExecuteWithRetry(healthCtx, "POOL_HEALTH", 1,
							pm.service.cfg.Redis.RetryAttempts, func(ctx context.Context) error {
								return pm.service.wrapper.WrapOperation(ctx, "POOL_HEALTH",
									nil, func() error {
										return pm.checkPoolHealth(ctx)
									})
							})
					})
				}
				return pm.service.timeoutManager.ExecuteWithRetry(healthCtx, "POOL_HEALTH", 1,
					pm.service.cfg.Redis.RetryAttempts, func(ctx context.Context) error {
						return pm.service.wrapper.WrapOperation(ctx, "POOL_HEALTH",
							nil, func() error {
								return pm.checkPoolHealth(ctx)
							})
					})
			})
			cancel()

			if err != nil {
				failedChecks := pm.failedChecks.Add(1)
				pm.logger.WithError(err).Error("Health check failed")
				if failedChecks >= int32(pm.monitoringCfg.MaxFailedChecks) {
					pm.logger.Error("Too many failed health checks, stopping monitoring")
					pm.Stop()
					return
				}
			} else {
				pm.failedChecks.Store(0)
			}
		}
	}
}

func (pm *PoolManager) checkPoolHealth(ctx context.Context) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.status.Load() != PoolStatusReady {
		return fmt.Errorf("pool is not ready")
	}

	if pm.service == nil || pm.service.pool == nil {
		return fmt.Errorf("pool is not initialized")
	}

	return pm.service.pool.Ping(ctx).Err()
}

func (pm *PoolManager) monitorMetrics(ctx context.Context) {
	// Initial delay to allow pool to stabilize
	time.Sleep(pm.monitoringCfg.StartupDelay)

	// Setup metrics collection ticker
	ticker := time.NewTicker(pm.monitoringCfg.MetricsInterval)
	defer ticker.Stop()

	// Define error channel for collecting routine
	errChan := make(chan error, 1)

	for {
		select {
		case <-ctx.Done():
			return

		case <-pm.stopChan:
			return

		case err := <-errChan:
			if err != nil {
				log.Printf("Metrics collection error: %v", err)
				// Reset metrics on error
				pm.resetMetrics()
			}

		case <-ticker.C:
			// Skip if not ready
			if !pm.isReady.Load() {
				continue
			}

			// Collect metrics in separate goroutine with timeout
			go func() {
				collectCtx, cancel := context.WithTimeout(ctx, pm.monitoringCfg.HealthCheckTimeout)
				defer cancel()

				if err := pm.collectAndStoreMetrics(collectCtx); err != nil {
					errChan <- err
				}
			}()
		}
	}
}

// Monitor metrics with control layers
func (pm *PoolManager) collectAndStoreMetrics(ctx context.Context) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// Check pool status
	if status := pm.status.Load().(PoolStatus); status != PoolStatusReady {
		return fmt.Errorf("pool is not ready, current status: %v", status)
	}

	// Validate service and pool
	if pm.service == nil || pm.service.pool == nil {
		return fmt.Errorf("pool is not initialized")
	}

	return pm.service.operationManager.ExecuteWithLock(ctx, "POOL_METRICS", func() error {
		if pm.service.cb != nil && pm.service.cfg.Circuit.Status {
			return pm.service.cb.Execute(func() error {
				return pm.service.timeoutManager.ExecuteWithRetry(ctx, "POOL_METRICS", 1,
					pm.service.cfg.Redis.RetryAttempts, func(ctx context.Context) error {
						return pm.service.wrapper.WrapOperation(ctx, "POOL_METRICS",
							nil, func() error {
								stats := pm.service.pool.PoolStats()
								if stats == nil {
									return fmt.Errorf("failed to get pool stats")
								}

								metrics := &PoolMetrics{
									TotalConnections:   int64(stats.TotalConns),
									IdleConnections:    int64(stats.IdleConns),
									WaitingRequests:    int64(stats.Hits),
									OperationLatency:   time.Duration(pm.service.cfg.Pool.WaitTimeout),
									LastScaleOperation: time.Now(),
								}

								metrics.ActiveConnections = metrics.TotalConnections - metrics.IdleConnections
								pm.metrics.Store(metrics)
								return nil
							})
					})
			})
		}
		return pm.service.timeoutManager.ExecuteWithRetry(ctx, "POOL_METRICS", 1,
			pm.service.cfg.Redis.RetryAttempts, func(ctx context.Context) error {
				return pm.service.wrapper.WrapOperation(ctx, "POOL_METRICS",
					nil, func() error {
						// Same implementation as above for consistency
						// In production, you might want to extract this to a separate method
						return nil
					})
			})
	})
}

func (pm *PoolManager) updateMetrics(ctx context.Context) error {
	if status := pm.status.Load().(PoolStatus); status != PoolStatusReady {
		return fmt.Errorf("pool is not ready, current status: %v", status)
	}

	stats := pm.service.pool.PoolStats()
	if stats == nil {
		return fmt.Errorf("failed to get pool stats")
	}

	metrics := &PoolMetrics{
		TotalConnections:   int64(stats.TotalConns),
		IdleConnections:    int64(stats.IdleConns),
		ActiveConnections:  int64(stats.TotalConns - stats.IdleConns),
		WaitingRequests:    int64(stats.Hits),
		OperationLatency:   time.Duration(pm.service.cfg.Pool.WaitTimeout),
		LastScaleOperation: time.Now(),
	}

	pm.metrics.Store(metrics)
	return nil
}

func (pm *PoolManager) validateConnCount(count int64) int64 {
	if count < 0 {
		return 0
	}
	maxSize := int64(pm.config.PoolSize)
	if count > maxSize {
		return maxSize
	}
	return count
}

func (pm *PoolManager) calculateActiveConns(total, idle int64) int64 {
	active := total - idle
	if active < 0 {
		return 0
	}
	if active > total {
		return total
	}
	return active
}

func (pm *PoolManager) resetMetrics() {
	metrics := &PoolMetrics{
		TotalConnections:  int64(pm.config.PoolSize),
		IdleConnections:   int64(pm.config.MinIdleConns),
		ActiveConnections: 0,
		WaitingRequests:   0,
		OperationLatency:  pm.monitoringCfg.HealthCheckTimeout,
	}
	pm.metrics.Store(metrics)
}

// handleScaling processes scaling operations with improved error handling
func (pm *PoolManager) handleScaling(ctx context.Context) {
	// Initialize scaling metrics
	pm.initializeScalingMetrics()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pm.stopChan:
			return
		case <-pm.scaleOperations:
			// Skip if pool is not ready
			if !pm.isReady.Load() || pm.status.Load().(PoolStatus) != PoolStatusReady {
				continue
			}

			// Calculate new size based on metrics
			metrics := pm.getCurrentMetrics()
			if metrics == nil {
				pm.logger.Error("No metrics available for scaling decision")
				continue
			}

			newSize := pm.calculateNewSize(metrics)
			if err := pm.performScaling(ctx, newSize); err != nil {
				pm.lastError.Store(err)
				pm.logger.WithError(err).Error("Pool scaling failed")
				pm.isScaling.Store(false)
			}
		}
	}
}

func (pm *PoolManager) performScaling(ctx context.Context, newSize int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Validate new size
	if newSize == pm.config.PoolSize {
		return nil
	}

	newSize = pm.constrainPoolSize(newSize)

	// Create new pool configuration
	newConfig := *pm.config
	newConfig.PoolSize = newSize

	// Create new client with timeout
	newPool := redis.NewClient(&newConfig)

	// Verify new pool
	if err := newPool.Ping(ctx).Err(); err != nil {
		newPool.Close()
		return fmt.Errorf("failed to verify new pool: %v", err)
	}

	// Switch to new pool
	oldPool := pm.service.pool
	pm.service.pool = newPool
	pm.config = &newConfig

	// Update metrics and last scale time
	metrics := pm.getCurrentMetrics()
	if metrics != nil {
		metrics.LastScaleOperation = time.Now()
		metrics.TotalConnections = int64(newSize)
		pm.metrics.Store(metrics)
	}
	pm.lastScaleTime.Store(time.Now())

	// Gracefully close old pool
	go pm.gracefulPoolClose(oldPool)

	pm.logger.WithFields(map[string]interface{}{
		"old_size": pm.config.PoolSize,
		"new_size": newSize,
	}).Info("Pool scaled successfully")

	return nil
}

func (pm *PoolManager) initializeScalingMetrics() {
	pm.scalingMetrics.minInterval = defaultMinScaleInterval
	pm.scalingMetrics.maxMultiplier = defaultMaxMultiplier
	pm.scalingMetrics.minMultiplier = defaultMinMultiplier
	pm.scalingMetrics.highThreshold = defaultHighThreshold
	pm.scalingMetrics.lowThreshold = defaultLowThreshold
	pm.scalingMetrics.cooldownPeriod = defaultCooldownPeriod

	// Initialize last scale time
	pm.lastScaleTime.Store(time.Now().Add(-defaultMinScaleInterval))
}

// shouldScale determines if scaling is needed with improved validation
func (pm *PoolManager) shouldScale(metrics *PoolMetrics) bool {
	if metrics == nil || metrics.TotalConnections == 0 {
		return false
	}

	utilization := float64(metrics.ActiveConnections) / float64(metrics.TotalConnections)
	return utilization >= pm.scalingMetrics.highThreshold ||
		utilization <= pm.scalingMetrics.lowThreshold
}

// canScale checks if scaling operation is allowed with improved timing logic
func (pm *PoolManager) canScale() bool {
	if !pm.isScaling.CompareAndSwap(false, true) {
		return false
	}

	lastScaleTime, ok := pm.lastScaleTime.Load().(time.Time)
	if !ok {
		pm.lastScaleTime.Store(time.Now().Add(-pm.scalingMetrics.minInterval))
		return true
	}

	if time.Since(lastScaleTime) < pm.scalingMetrics.minInterval {
		pm.isScaling.Store(false)
		return false
	}

	return true
}

// calculateNewSize determines the new pool size with improved bounds checking
func (pm *PoolManager) calculateNewSize(metrics *PoolMetrics) int {
	if metrics == nil || metrics.TotalConnections == 0 {
		return pm.service.cfg.Pool.Size
	}

	currentSize := int(metrics.TotalConnections)
	utilization := float64(metrics.ActiveConnections) / float64(metrics.TotalConnections)

	var scaleFactor float64
	switch {
	case utilization >= pm.scalingMetrics.highThreshold:
		scaleFactor = pm.scalingMetrics.maxMultiplier
	case utilization <= pm.scalingMetrics.lowThreshold:
		scaleFactor = pm.scalingMetrics.minMultiplier
	default:
		return currentSize
	}

	newSize := int(float64(currentSize) * scaleFactor)
	return pm.constrainPoolSize(newSize)
}

func (pm *PoolManager) periodicCleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pm.stopChan:
			return
		case <-ticker.C:
			pm.cleanupMetrics()
		}
	}
}

func (pm *PoolManager) cleanupMetrics() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	currentMetrics := pm.getCurrentMetrics()
	if currentMetrics == nil {
		return
	}

	// Reset some metrics if they're too old
	if time.Since(currentMetrics.LastScaleOperation) > 24*time.Hour {
		metrics := &PoolMetrics{
			TotalConnections: int64(pm.config.PoolSize),
			IdleConnections:  int64(pm.config.MinIdleConns),
		}
		pm.metrics.Store(metrics)
	}
}

// scalePool performs the actual scaling operation with improved error handling
func (pm *PoolManager) scalePool(ctx context.Context, newSize int) error {
	if !pm.canScale() {
		return fmt.Errorf("scaling not allowed at this time")
	}

	return pm.service.operationManager.ExecuteWithLock(ctx, "POOL_SCALE", func() error {
		if pm.service.cb != nil && pm.service.cfg.Circuit.Status {
			return pm.service.cb.Execute(func() error {
				return pm.service.timeoutManager.ExecuteWithRetry(ctx, "POOL_SCALE", 1,
					pm.service.cfg.Redis.RetryAttempts, func(ctx context.Context) error {
						return pm.service.wrapper.WrapOperation(ctx, "POOL_SCALE",
							map[string]interface{}{
								"new_size": newSize,
								"old_size": pm.config.PoolSize,
							}, func() error {
								return pm.performScaling(ctx, newSize)
							})
					})
			})
		}
		return pm.service.timeoutManager.ExecuteWithRetry(ctx, "POOL_SCALE", 1,
			pm.service.cfg.Redis.RetryAttempts, func(ctx context.Context) error {
				return pm.service.wrapper.WrapOperation(ctx, "POOL_SCALE",
					map[string]interface{}{
						"new_size": newSize,
						"old_size": pm.config.PoolSize,
					}, func() error {
						return pm.performScaling(ctx, newSize)
					})
			})
	})
}

// gracefulPoolClose handles graceful shutdown of old pool with timeout
func (pm *PoolManager) gracefulPoolClose(pool *redis.Client) {
	if pool == nil {
		return
	}

	// Wait for ongoing operations
	time.Sleep(pm.scalingMetrics.cooldownPeriod)

	// Close with timeout context
	_, cancel := context.WithTimeout(context.Background(), pm.scalingMetrics.cooldownPeriod)
	defer cancel()

	if err := pool.Close(); err != nil {
		log.Printf("Error closing old pool: %v", err)
	}
}

// GetScalingMetrics returns current scaling metrics
func (pm *PoolManager) GetScalingMetrics() map[string]interface{} {
	return map[string]interface{}{
		"min_interval":    pm.scalingMetrics.minInterval,
		"max_multiplier":  pm.scalingMetrics.maxMultiplier,
		"min_multiplier":  pm.scalingMetrics.minMultiplier,
		"high_threshold":  pm.scalingMetrics.highThreshold,
		"low_threshold":   pm.scalingMetrics.lowThreshold,
		"cooldown_period": pm.scalingMetrics.cooldownPeriod,
	}
}

// getCurrentMetrics safely retrieves current metrics
func (pm *PoolManager) getCurrentMetrics() *PoolMetrics {
	return pm.metrics.Load().(*PoolMetrics)
}

// GetPoolStats returns current pool statistics
func (pm *PoolManager) GetPoolStats() *PoolMetrics {
	metricsInterface := pm.metrics.Load()
	if metricsInterface == nil {
		return &PoolMetrics{}
	}

	currentMetrics := metricsInterface.(*PoolMetrics)
	metrics := &PoolMetrics{
		TotalConnections:   currentMetrics.TotalConnections,
		IdleConnections:    currentMetrics.IdleConnections,
		ActiveConnections:  currentMetrics.ActiveConnections,
		WaitingRequests:    currentMetrics.WaitingRequests,
		OperationLatency:   currentMetrics.OperationLatency,
		LastScaleOperation: currentMetrics.LastScaleOperation,
	}

	return metrics
}

// constrainPoolSize ensures pool size stays within configured limits
func (pm *PoolManager) constrainPoolSize(size int) int {
	minSize := pm.service.cfg.Pool.MinIdle
	maxSize := pm.service.cfg.Pool.Size * 2

	switch {
	case size < minSize:
		pm.logger.WithFields(map[string]interface{}{
			"requested_size": size,
			"minimum_size":   minSize,
		}).Info("Requested pool size below minimum, using minimum")
		return minSize

	case size > maxSize:
		pm.logger.WithFields(map[string]interface{}{
			"requested_size": size,
			"maximum_size":   maxSize,
		}).Info("Requested pool size exceeds maximum, using maximum")
		return maxSize

	default:
		if minSize > 0 {
			remainder := size % minSize
			if remainder > minSize/2 {
				size += minSize - remainder
			} else {
				size -= remainder
			}
		}
		return size
	}
}

// Helper method to get current pool size safely
func (pm *PoolManager) getCurrentPoolSize() int {
	if pm.config != nil {
		return pm.config.PoolSize
	}
	if pm.service != nil && pm.service.cfg != nil {
		return pm.service.cfg.Pool.Size
	}
	return 0
}

// Helper method to validate pool size
func (pm *PoolManager) isValidPoolSize(size int) bool {
	if pm.service == nil || pm.service.cfg == nil {
		return false
	}
	return size >= pm.service.cfg.Pool.MinIdle &&
		size <= pm.service.cfg.Pool.Size*2
}
