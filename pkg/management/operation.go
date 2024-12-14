// pkg/management/operation.go
package management

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Add metrics structure for operation manager
type operationMetrics struct {
	operationLatency time.Duration
	operationCount   int64
	errorCount       int64
}

// Update OperationManager struct to include metrics
type OperationManager struct {
	service            *RedisService
	maintenanceManager *MaintenanceManager
	shutdownManager    *ShutdownManager
	metrics            *operationMetrics
	mu                 sync.RWMutex
}

// OperationStatus represents the combined status of maintenance and shutdown
type OperationStatus struct {
	// Maintenance status
	IsMaintenanceMode bool
	MaintenanceReason string
	ReadOnlyMode      bool
	RemainingTime     time.Duration

	// Shutdown status
	IsShuttingDown    bool
	ActiveOperations  int64
	CompletedOps      int64
	ShutdownStartTime time.Time
	ShutdownDuration  time.Duration

	// Performance metrics
	OperationLatency time.Duration
	SuccessRate      float64
	ErrorRate        float64
}

// NewOperationManager creates a new operation manager
func NewOperationManager(service *RedisService) (*OperationManager, error) {
	// Initialize maintenance manager
	maintenanceManager := NewMaintenanceManager(service)
	if maintenanceManager == nil {
		return nil, fmt.Errorf("maintenance manager is not initialized")
	}

	// Initialize operation manager with enhanced shutdown configuration
	shutdownConfig := ShutdownConfig{
		Timeout:        service.cfg.Redis.ShutdownTimeout,
		RetryAttempts:  service.cfg.Redis.RetryAttempts,
		RetryDelay:     service.cfg.Redis.RetryDelay,
		CleanupTimeout: time.Second * 30,
	}

	logger := initializeLogger(service.cfg) // Implement this based on your logging needs

	// Initialize shutdown manager with configuration
	shutdownManager := NewShutdownManager(service, shutdownConfig, logger)
	if shutdownManager == nil {
		return nil, fmt.Errorf("Shutdown Manager is not initialized")
	}

	return &OperationManager{
		service:            service,
		shutdownManager:    shutdownManager,
		maintenanceManager: maintenanceManager,
		metrics:            &operationMetrics{},
	}, nil
}

// UpdateMetrics updates operation metrics
func (om *OperationManager) UpdateMetrics(latency time.Duration, success bool) {
	atomic.AddInt64(&om.metrics.operationCount, 1)
	atomic.StoreInt64((*int64)(&om.metrics.operationLatency), int64(latency))

	if !success {
		atomic.AddInt64(&om.metrics.errorCount, 1)
	}
}

// checkOperation verifies if an operation is allowed
func (om *OperationManager) checkOperation(ctx context.Context, cmd string) error {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if !om.shutdownManager.TrackOperation(ctx) {
		return fmt.Errorf("service is shutting down or operation cancelled")
	}
	defer om.shutdownManager.FinishOperation()

	if om.maintenanceManager == nil {
		return fmt.Errorf("maintenance manager is not initialized")
	}

	if om.maintenanceManager == nil || !om.maintenanceManager.IsOperationAllowed(cmd) {
		return fmt.Errorf("operation %s not allowed during maintenance mode", cmd)
	}

	return nil
}

// GetMaintenanceManager returns the maintenance manager
func (om *OperationManager) GetMaintenanceManager() *MaintenanceManager {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return om.maintenanceManager
}

// GetShutdownManager returns the shutdown manager
func (om *OperationManager) GetShutdownManager() *ShutdownManager {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return om.shutdownManager
}

// GetStatus returns enhanced combined status of maintenance and shutdown
func (om *OperationManager) GetStatus() OperationStatus {
	om.mu.RLock()
	defer om.mu.RUnlock()

	// Get maintenance status
	maintStatus := om.maintenanceManager.GetMaintenanceStatus()

	// Get shutdown metrics
	shutdownMetrics := om.shutdownManager.GetMetrics()

	// Calculate operation rates
	var successRate, errorRate float64
	totalOps := shutdownMetrics.CompletedOps + shutdownMetrics.RemainingOps
	if totalOps > 0 {
		successRate = float64(shutdownMetrics.CompletedOps) / float64(totalOps) * 100
		errorRate = float64(shutdownMetrics.RemainingOps) / float64(totalOps) * 100
	}

	return OperationStatus{
		// Maintenance information
		IsMaintenanceMode: maintStatus.IsMaintenanceMode,
		MaintenanceReason: maintStatus.Reason,
		ReadOnlyMode:      maintStatus.ReadOnlyMode,
		RemainingTime:     maintStatus.RemainingTime,

		// Shutdown information
		IsShuttingDown:    om.shutdownManager.IsShuttingDown(),
		ActiveOperations:  om.shutdownManager.GetOperationCount(),
		CompletedOps:      shutdownMetrics.CompletedOps,
		ShutdownStartTime: shutdownMetrics.StartTime,
		ShutdownDuration:  shutdownMetrics.ShutdownDuration,

		// Performance metrics
		OperationLatency: time.Duration(atomic.LoadInt64((*int64)(&om.metrics.operationLatency))),
		SuccessRate:      successRate,
		ErrorRate:        errorRate,
	}
}
