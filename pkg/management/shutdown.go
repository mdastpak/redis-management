package management

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ShutdownHooks defines hooks that run before and after shutdown
type ShutdownHooks struct {
	PreShutdown  func() error
	PostShutdown func() error
}

// ShutdownConfig contains configuration for shutdown behavior
type ShutdownConfig struct {
	Timeout        time.Duration
	RetryAttempts  int
	RetryDelay     time.Duration
	CleanupTimeout time.Duration
}

// ShutdownMetrics tracks metrics during shutdown
type ShutdownMetrics struct {
	StartTime        time.Time
	CompletionTime   time.Time
	RemainingOps     int64
	CompletedOps     int64
	ShutdownDuration time.Duration
}

// ShutdownManager handles graceful service shutdown
type ShutdownManager struct {
	service        *RedisService
	operationCount int64
	shutdownCh     chan struct{}
	timeout        time.Duration
	retryAttempts  int
	retryDelay     time.Duration
	cleanupTimeout time.Duration
	isShuttingDown int32
	wg             sync.WaitGroup
	hooks          ShutdownHooks
	metrics        *ShutdownMetrics
	logger         Logger // Interface for logging
	mu             sync.RWMutex
}

// Logger interface for shutdown manager
type Logger interface {
	Info(msg string)
	Error(msg string, err error)
	Warn(msg string)
}

// NewShutdownManager creates a new shutdown manager with enhanced configuration
func NewShutdownManager(service *RedisService, config ShutdownConfig, logger Logger) *ShutdownManager {
	if logger == nil {
		logger = &defaultLogger{} // Implement a default logger
	}

	return &ShutdownManager{
		service:        service,
		timeout:        config.Timeout,
		retryAttempts:  config.RetryAttempts,
		retryDelay:     config.RetryDelay,
		cleanupTimeout: config.CleanupTimeout,
		shutdownCh:     make(chan struct{}),
		logger:         logger,
		metrics:        &ShutdownMetrics{},
	}
}

// TrackOperation tracks a new operation with context support
func (sm *ShutdownManager) TrackOperation(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		sm.logger.Warn("Operation tracking cancelled by context")
		return false
	case <-sm.shutdownCh:
		sm.logger.Warn("Operation rejected - shutdown in progress")
		return false
	default:
		if atomic.LoadInt32(&sm.isShuttingDown) == 1 {
			return false
		}
		atomic.AddInt64(&sm.operationCount, 1)
		sm.wg.Add(1)
		return true
	}
}

// FinishOperation marks an operation as complete
func (sm *ShutdownManager) FinishOperation() {
	atomic.AddInt64(&sm.operationCount, -1)
	sm.wg.Done()
	atomic.AddInt64(&sm.metrics.CompletedOps, 1)
}

// Shutdown initiates graceful shutdown with enhanced error handling and metrics
func (sm *ShutdownManager) Shutdown(ctx context.Context) error {
	sm.mu.Lock()
	if !atomic.CompareAndSwapInt32(&sm.isShuttingDown, 0, 1) {
		remainingOps := atomic.LoadInt64(&sm.operationCount)
		sm.mu.Unlock()
		if remainingOps > 0 {
			sm.logger.Info(fmt.Sprintf("Shutdown scheduled, waiting for %d operations to complete", remainingOps))
			// Join the waiting for completion
			return sm.waitForCompletion(ctx)
		}
		sm.logger.Info("Shutdown already completed")
		return nil
	}

	sm.metrics.StartTime = time.Now()
	sm.logger.Info("Starting graceful shutdown")
	close(sm.shutdownCh) // Prevent new operations
	sm.mu.Unlock()

	return sm.waitForCompletion(ctx)
}

func (sm *ShutdownManager) waitForCompletion(ctx context.Context) error {
	complete := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(complete)
	}()

	select {
	case <-complete:
		sm.logger.Info("All operations completed successfully")
		sm.metrics.CompletionTime = time.Now()
		sm.metrics.ShutdownDuration = sm.metrics.CompletionTime.Sub(sm.metrics.StartTime)
		return nil
	case <-ctx.Done():
		remainingOps := atomic.LoadInt64(&sm.operationCount)
		if remainingOps > 0 {
			return fmt.Errorf("shutdown timed out with %d operations remaining", remainingOps)
		}
		sm.logger.Info("All operations completed successfully")
		return nil
	}
}

// cleanup handles resource cleanup during shutdown
func (sm *ShutdownManager) cleanup(ctx context.Context) error {
	// Implement resource cleanup logic
	return nil
}

// IsShuttingDown returns whether shutdown is in progress
func (sm *ShutdownManager) IsShuttingDown() bool {
	return atomic.LoadInt32(&sm.isShuttingDown) == 1
}

// GetOperationCount returns current operation count
func (sm *ShutdownManager) GetOperationCount() int64 {
	return atomic.LoadInt64(&sm.operationCount)
}

// GetMetrics returns current shutdown metrics
func (sm *ShutdownManager) GetMetrics() ShutdownMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.metrics == nil {
		return ShutdownMetrics{}
	}

	// Return a copy to prevent external modification
	return *sm.metrics

}

// SetShutdownHooks sets pre and post shutdown hooks
func (sm *ShutdownManager) SetShutdownHooks(hooks ShutdownHooks) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.hooks = hooks
}

// Default logger implementation
type defaultLogger struct{}

func (l *defaultLogger) Info(msg string) {
	log.Printf("[INFO] %s", msg)
}

func (l *defaultLogger) Error(msg string, err error) {
	log.Printf("[ERROR] %s: %v", msg, err)
}

func (l *defaultLogger) Warn(msg string) {
	log.Printf("[WARN] %s", msg)
}
