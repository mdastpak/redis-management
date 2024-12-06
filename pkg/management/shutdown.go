package management

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ShutdownManager handles graceful service shutdown
type ShutdownManager struct {
	service        *RedisService
	operationCount int64
	shutdownCh     chan struct{}
	timeout        time.Duration
	isShuttingDown int32
	wg             sync.WaitGroup
}

// NewShutdownManager creates a new shutdown manager
func NewShutdownManager(service *RedisService, timeout time.Duration) *ShutdownManager {
	return &ShutdownManager{
		service:    service,
		timeout:    timeout,
		shutdownCh: make(chan struct{}),
	}
}

// TrackOperation increments operation counter if not shutting down
func (sm *ShutdownManager) TrackOperation() bool {
	if atomic.LoadInt32(&sm.isShuttingDown) == 1 {
		return false
	}
	atomic.AddInt64(&sm.operationCount, 1)
	sm.wg.Add(1)
	return true
}

// FinishOperation decrements operation counter
func (sm *ShutdownManager) FinishOperation() {
	atomic.AddInt64(&sm.operationCount, -1)
	sm.wg.Done()
}

// GetOperationCount returns current operation count
func (sm *ShutdownManager) GetOperationCount() int64 {
	return atomic.LoadInt64(&sm.operationCount)
}

// IsShuttingDown returns whether shutdown is in progress
func (sm *ShutdownManager) IsShuttingDown() bool {
	return atomic.LoadInt32(&sm.isShuttingDown) == 1
}

// StartBulkProcessor starts the bulk processor with shutdown awareness
func (sm *ShutdownManager) StartBulkProcessor(ctx context.Context) {
	processor := NewBulkProcessor(sm.service)

	// Create a context that will be cancelled on shutdown
	shutdownCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-sm.shutdownCh:
			cancel()
		case <-ctx.Done():
			cancel()
		}
	}()

	processor.Start(shutdownCtx)
}

// Shutdown initiates graceful shutdown
func (sm *ShutdownManager) Shutdown(ctx context.Context) error {
	// Prevent new operations
	if !atomic.CompareAndSwapInt32(&sm.isShuttingDown, 0, 1) {
		return fmt.Errorf("shutdown already in progress")
	}
	close(sm.shutdownCh)

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, sm.timeout)
	defer cancel()

	// Wait for completion or timeout
	completeCh := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(completeCh)
	}()

	select {
	case <-completeCh:
		// All operations completed successfully
		return nil
	case <-timeoutCtx.Done():
		remainingOps := atomic.LoadInt64(&sm.operationCount)
		return fmt.Errorf("shutdown timed out with %d operations remaining", remainingOps)
	}
}
