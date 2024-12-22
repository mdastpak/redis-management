package timeout

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"redis-management/config"
)

// OperationStats holds statistics for operation timing
type OperationStats struct {
	Count       int64
	TotalTime   time.Duration
	AverageTime time.Duration
	LastUpdated time.Time
	MinTime     time.Duration
	MaxTime     time.Duration
}

// Manager handles timeout management for Redis operations
type Manager struct {
	config      *config.TimeoutConfig
	stats       sync.Map // map[string]*OperationStats
	mu          sync.RWMutex
	lastCleanup time.Time
}

// NewManager creates a new timeout manager
func NewManager(cfg *config.TimeoutConfig) *Manager {
	if cfg == nil {
		fmt.Println("timeout config is nil")
		return nil
	}

	tm := &Manager{
		config:      cfg,
		lastCleanup: time.Now(),
	}

	// Start cleanup goroutine if adaptive timeout is enabled
	if cfg.Adaptive.Enabled {
		go tm.periodicCleanup()
	}

	return tm
}

// GetTimeout calculates the appropriate timeout for an operation
func (tm *Manager) GetTimeout(operation string, items int) time.Duration {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	baseTimeout := tm.getBaseTimeout(operation)
	timeout := tm.adjustTimeout(operation, baseTimeout, items)
	// fmt.Printf("GetTimeout: operation=%s, items=%d, baseTimeout=%d, timeout=%d\n", operation, items, baseTimeout, timeout)

	// Special handling for bulk operations
	if strings.HasPrefix(operation, "BULK_") {
		// Use a more reasonable timeout for bulk operations
		timeout = max(timeout, time.Duration(items)*baseTimeout)
	}

	// Enforce minimum timeout
	if timeout < tm.config.MinTimeout {
		return tm.config.MinTimeout
	}

	// Allow longer maximum timeout for bulk operations
	maxTimeout := tm.config.MaxTimeout
	if strings.HasPrefix(operation, "BULK_") {
		maxTimeout = maxTimeout * 2
	}

	if timeout > maxTimeout {
		return maxTimeout
	}

	return timeout
}

// RecordTiming records the timing for an operation
func (tm *Manager) RecordTiming(operation string, duration time.Duration) {
	if !tm.config.Adaptive.Enabled {
		return
	}

	statsInterface, _ := tm.stats.LoadOrStore(operation, &OperationStats{
		MinTime: duration,
		MaxTime: duration,
	})

	stats := statsInterface.(*OperationStats)

	tm.mu.Lock()
	defer tm.mu.Unlock()

	stats.Count++
	stats.TotalTime += duration
	stats.AverageTime = time.Duration(int64(stats.TotalTime) / stats.Count)
	stats.LastUpdated = time.Now()

	if duration < stats.MinTime {
		stats.MinTime = duration
	}
	if duration > stats.MaxTime {
		stats.MaxTime = duration
	}
}

// ExecuteWithTimeout executes a function with appropriate timeout
func (tm *Manager) ExecuteWithTimeout(ctx context.Context, operation string, items int, fn func(context.Context) error) error {
	timeout := tm.GetTimeout(operation, items)

	// fmt.Printf("Timeout manager - Operation: %s, Items: %d, Calculated timeout: %v",
	// operation, items, timeout)

	// Create a new context with longer timeout for bulk operations
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout*2) // Double timeout for bulk operations
	// fmt.Printf("ExecuteWithTimeout: timeoutCtx=%v, cancel=%v, operation=%s, items=%d, timeout=%d\n", timeoutCtx, cancel, operation, items, timeout)
	defer cancel()

	start := time.Now()
	err := fn(timeoutCtx)
	duration := time.Since(start)

	tm.RecordTiming(operation, duration)

	if timeoutCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("ExecuteWithTimeout - operation %s timed out after %v (duration: %v)",
			operation, timeout, duration)
	}

	return err
}

// ExecuteWithRetry executes a function with retries and backoff
func (tm *Manager) ExecuteWithRetry(ctx context.Context, operation string, items int, maxRetries int, fn func(context.Context) error) error {
	var lastErr error
	backoffDuration := tm.config.MinTimeout

	for retry := 0; retry <= maxRetries; retry++ {
		err := tm.ExecuteWithTimeout(ctx, operation, items, fn)
		if err == nil {
			return nil
		}

		lastErr = err
		if retry == maxRetries {
			break
		}

		// Calculate backoff duration
		backoffDuration = time.Duration(float64(backoffDuration) * tm.config.BackoffFactor)
		if backoffDuration > tm.config.MaxTimeout {
			backoffDuration = tm.config.MaxTimeout
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffDuration):
			continue
		}
	}

	return fmt.Errorf("operation %s failed after %d retries: %w", operation, maxRetries, lastErr)
}

// GetStats returns current timing statistics for operations
func (tm *Manager) GetStats() map[string]OperationStats {
	results := make(map[string]OperationStats)
	tm.stats.Range(func(key, value interface{}) bool {
		results[key.(string)] = *value.(*OperationStats)
		return true
	})
	return results
}

// internal helper methods

func (tm *Manager) getBaseTimeout(operation string) time.Duration {
	switch operation {
	case "GET":
		return tm.config.Operations.Get
	case "SET":
		return tm.config.Operations.Set
	case "DELETE":
		return tm.config.Operations.Delete
	case "BULK":
		return tm.config.Operations.BulkBase
	default:
		return tm.config.BaseTimeout
	}
}

func (tm *Manager) adjustTimeout(operation string, baseTimeout time.Duration, items int) time.Duration {
	if !tm.config.Adaptive.Enabled {
		return baseTimeout * time.Duration(items)
	}

	statsInterface, ok := tm.stats.Load(operation)
	if !ok {
		return baseTimeout * time.Duration(items)
	}

	stats := statsInterface.(*OperationStats)
	if stats.Count < int64(tm.config.Adaptive.WindowSize) {
		return baseTimeout * time.Duration(items)
	}

	adjustedTimeout := stats.AverageTime * time.Duration(items)
	maxAdjustment := time.Duration(float64(baseTimeout) * (1 + tm.config.Adaptive.MaxAdjustment))

	if adjustedTimeout > maxAdjustment {
		return maxAdjustment
	}
	return adjustedTimeout
}

func (tm *Manager) periodicCleanup() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		tm.cleanup()
	}
}

func (tm *Manager) cleanup() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	cutoff := time.Now().Add(-tm.config.Adaptive.HistoryRetention)
	var keysToDelete []string

	tm.stats.Range(func(key, value interface{}) bool {
		stats := value.(*OperationStats)
		if stats.LastUpdated.Before(cutoff) {
			keysToDelete = append(keysToDelete, key.(string))
		}
		return true
	})

	for _, key := range keysToDelete {
		tm.stats.Delete(key)
	}

	tm.lastCleanup = time.Now()
}

func (tm *Manager) CreateContext(parent context.Context, operation string) context.Context {
	timeout := tm.GetTimeout(operation, 1)
	ctx, _ := context.WithTimeout(parent, timeout)
	return ctx
}
