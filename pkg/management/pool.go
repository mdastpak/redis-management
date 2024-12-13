// pkg/management/pool.go

package management

import (
	"context"
	"fmt"
	"log"
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
	// Initialize Redis options
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

	// Initialize pool
	rs.pool = redis.NewClient(options)

	// Create monitoring configuration
	monitoringCfg := MonitoringConfig{
		HealthCheckInterval: time.Second,
		MetricsInterval:     time.Second,
		StartupDelay:        100 * time.Millisecond,
		HealthCheckTimeout:  500 * time.Millisecond,
		MaxFailedChecks:     3,
	}

	// Create pool manager
	pm := &PoolManager{
		service:         rs,
		config:          options,
		monitoringCfg:   monitoringCfg,
		scaleOperations: make(chan struct{}, 1),
		stopChan:        make(chan struct{}),
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

	rs.poolManager = pm

	// Initial connection test
	if err := rs.pool.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to ping Redis: %v", err)
	}

	// Start monitoring with startup sequence
	if err := pm.StartMonitoring(ctx); err != nil {
		return fmt.Errorf("failed to start monitoring: %v", err)
	}

	return nil
}

func (pm *PoolManager) Stop() {
	if !pm.status.CompareAndSwap(PoolStatusReady, PoolStatusStopping) {
		return // Already stopping or stopped
	}

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

	// Perform initial health check
	if err := pm.checkPoolHealth(ctx); err != nil {
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

	// Wait for monitoring to be fully established
	time.Sleep(pm.monitoringCfg.StartupDelay)
	pm.isReady.Store(true)

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
			err := pm.checkPoolHealth(healthCtx)
			cancel()

			if err != nil {
				failedChecks := pm.failedChecks.Add(1)
				log.Printf("Health check failed (%d/%d): %v",
					failedChecks, pm.monitoringCfg.MaxFailedChecks, err)

				if failedChecks >= int32(pm.monitoringCfg.MaxFailedChecks) {
					log.Printf("Too many failed health checks, stopping monitoring")
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

	// Get pool stats with timeout
	var stats *redis.PoolStats
	done := make(chan struct{})

	go func() {
		defer close(done)
		stats = pm.service.pool.PoolStats()
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("timeout getting pool stats")
	case <-done:
		if stats == nil {
			return fmt.Errorf("failed to get pool stats")
		}
	}

	// Get pool options safely
	var poolTimeout time.Duration
	if options := pm.service.pool.Options(); options != nil {
		poolTimeout = time.Duration(options.PoolTimeout)
	} else {
		poolTimeout = pm.monitoringCfg.HealthCheckTimeout
	}

	// Calculate metrics with validation
	metrics := &PoolMetrics{
		TotalConnections:   pm.validateConnCount(int64(stats.TotalConns)),
		IdleConnections:    pm.validateConnCount(int64(stats.IdleConns)),
		WaitingRequests:    int64(stats.Hits),
		OperationLatency:   poolTimeout,
		LastScaleOperation: time.Now(),
	}

	// Calculate active connections
	metrics.ActiveConnections = pm.calculateActiveConns(
		metrics.TotalConnections,
		metrics.IdleConnections,
	)

	// Store metrics if still ready
	if pm.status.Load().(PoolStatus) == PoolStatusReady {
		pm.metrics.Store(metrics)
		return nil
	}

	return fmt.Errorf("pool status changed during metrics collection")
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

			if err := pm.performScaling(ctx); err != nil {
				pm.lastError.Store(err)
				log.Printf("Pool scaling failed: %v", err)
				// Reset scaling state
				pm.isScaling.Store(false)
			}
		}
	}
}

func (pm *PoolManager) performScaling(ctx context.Context) error {
	// Check if scaling is allowed
	if !pm.canScale() {
		return fmt.Errorf("scaling not allowed at this time")
	}

	// Get current metrics
	metrics := pm.getCurrentMetrics()
	if metrics == nil {
		return fmt.Errorf("no metrics available")
	}

	// Check if scaling is needed
	if !pm.shouldScale(metrics) {
		pm.isScaling.Store(false)
		return nil
	}

	// Calculate new size
	newSize := pm.calculateNewSize(metrics)
	if newSize == int(metrics.TotalConnections) {
		pm.isScaling.Store(false)
		return nil
	}

	// Perform scaling operation
	err := pm.scalePool(ctx, newSize)
	pm.isScaling.Store(false)
	return err
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

// scalePool performs the actual scaling operation with improved error handling
func (pm *PoolManager) scalePool(ctx context.Context, newSize int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Validate context
	if ctx.Err() != nil {
		return fmt.Errorf("context cancelled before scaling: %v", ctx.Err())
	}

	// Validate new size
	if newSize == pm.service.cfg.Pool.Size {
		return nil // No scaling needed
	}

	// Create new pool configuration
	newConfig := *pm.config
	newConfig.PoolSize = newSize

	// Create new client with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, pm.monitoringCfg.HealthCheckTimeout)
	defer cancel()

	newPool := redis.NewClient(&newConfig)

	// Verify new pool
	if err := newPool.Ping(timeoutCtx).Err(); err != nil {
		newPool.Close()
		return fmt.Errorf("failed to verify new pool: %v", err)
	}

	// Switch to new pool
	oldPool := pm.service.pool
	pm.service.pool = newPool

	// Update metrics and last scale time
	metrics := pm.getCurrentMetrics()
	if metrics != nil {
		metrics.LastScaleOperation = time.Now()
		pm.metrics.Store(metrics)
	}
	pm.lastScaleTime.Store(time.Now())

	// Gracefully close old pool
	go pm.gracefulPoolClose(oldPool)

	log.Printf("Pool scaled from %d to %d connections",
		pm.config.PoolSize, newSize)
	return nil
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
	return pm.getCurrentMetrics()
}

// constrainPoolSize ensures pool size stays within configured limits
func (pm *PoolManager) constrainPoolSize(size int) int {
	// Get configuration limits with safety checks
	if pm.service == nil || pm.service.cfg == nil {
		// If config is not available, return current pool size
		if pm.config != nil {
			return pm.config.PoolSize
		}
		return size
	}

	minSize := pm.service.cfg.Pool.MinIdle
	maxSize := pm.service.cfg.Pool.Size * 2 // Maximum allowed is double the configured size

	switch {
	case size < minSize:
		log.Printf("Requested pool size %d is below minimum %d, using minimum",
			size, minSize)
		return minSize

	case size > maxSize:
		log.Printf("Requested pool size %d exceeds maximum %d, using maximum",
			size, maxSize)
		return maxSize

	default:
		// Round to nearest multiple of MinIdle for better resource management
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
