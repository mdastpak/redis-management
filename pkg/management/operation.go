// pkg/management/operation.go
package management

import (
	"context"
	"fmt"
	"time"
)

// OperationManager handles operation lifecycle and permissions
type OperationManager struct {
	service            *RedisService
	maintenanceManager *MaintenanceManager
	shutdownManager    *ShutdownManager
}

// NewOperationManager creates a new operation manager
func NewOperationManager(service *RedisService) *OperationManager {
	return &OperationManager{
		service:            service,
		maintenanceManager: NewMaintenanceManager(service),
		shutdownManager:    NewShutdownManager(service, service.cfg.Redis.ShutdownTimeout),
	}
}

// ExecuteWithLock executes an operation with proper locking and checks
func (om *OperationManager) ExecuteWithLock(ctx context.Context, cmd string, operation func() error) error {
	if !om.shutdownManager.TrackOperation() {
		return fmt.Errorf("service is shutting down")
	}
	defer om.shutdownManager.FinishOperation()

	if err := om.checkOperation(cmd); err != nil {
		return err
	}

	if !om.maintenanceManager.IsOperationAllowed(cmd) {
		return fmt.Errorf("operation %s not allowed during maintenance mode", cmd)
	}

	return operation()
}

// ExecuteReadOp executes a read operation that returns a string
func (om *OperationManager) ExecuteReadOp(ctx context.Context, cmd string, operation func() (string, error)) (string, error) {
	if !om.shutdownManager.TrackOperation() {
		return "", fmt.Errorf("service is shutting down")
	}
	defer om.shutdownManager.FinishOperation()

	if err := om.checkOperation(cmd); err != nil {
		return "", err
	}

	if !om.maintenanceManager.IsOperationAllowed(cmd) {
		return "", fmt.Errorf("operation %s not allowed during maintenance mode", cmd)
	}

	return operation()
}

// ExecuteDurationOp executes an operation that returns a time.Duration
func (om *OperationManager) ExecuteDurationOp(ctx context.Context, cmd string, operation func() (time.Duration, error)) (time.Duration, error) {
	if !om.shutdownManager.TrackOperation() {
		return 0, fmt.Errorf("service is shutting down")
	}
	defer om.shutdownManager.FinishOperation()

	if err := om.checkOperation(cmd); err != nil {
		return 0, err
	}

	if !om.maintenanceManager.IsOperationAllowed(cmd) {
		return 0, fmt.Errorf("operation %s not allowed during maintenance mode", cmd)
	}

	return operation()
}

// ExecuteBatchOp executes a batch operation that returns a string map
func (om *OperationManager) ExecuteBatchOp(ctx context.Context, cmd string, operation func() (map[string]string, error)) (map[string]string, error) {
	if !om.shutdownManager.TrackOperation() {
		return nil, fmt.Errorf("service is shutting down")
	}
	defer om.shutdownManager.FinishOperation()

	if err := om.checkOperation(cmd); err != nil {
		return nil, err
	}

	if !om.maintenanceManager.IsOperationAllowed(cmd) {
		return nil, fmt.Errorf("operation %s not allowed during maintenance mode", cmd)
	}

	return operation()
}

// ExecuteBatchDurationOp executes a batch operation that returns a duration map
func (om *OperationManager) ExecuteBatchDurationOp(ctx context.Context, cmd string, operation func() (map[string]time.Duration, error)) (map[string]time.Duration, error) {
	if !om.shutdownManager.TrackOperation() {
		return nil, fmt.Errorf("service is shutting down")
	}
	defer om.shutdownManager.FinishOperation()

	if err := om.checkOperation(cmd); err != nil {
		return nil, err
	}

	if !om.maintenanceManager.IsOperationAllowed(cmd) {
		return nil, fmt.Errorf("operation %s not allowed during maintenance mode", cmd)
	}

	return operation()
}

// checkOperation verifies if an operation is allowed
func (om *OperationManager) checkOperation(cmd string) error {
	if om.maintenanceManager == nil {
		return nil
	}

	if !om.maintenanceManager.IsOperationAllowed(cmd) {
		return fmt.Errorf("operation %s not allowed during maintenance mode", cmd)
	}

	return nil
}

// GetMaintenanceManager returns the maintenance manager
func (om *OperationManager) GetMaintenanceManager() *MaintenanceManager {
	return om.maintenanceManager
}

// GetShutdownManager returns the shutdown manager
func (om *OperationManager) GetShutdownManager() *ShutdownManager {
	return om.shutdownManager
}

// GetOperationStatus returns combined status of maintenance and shutdown
type OperationStatus struct {
	IsMaintenanceMode bool
	IsShuttingDown    bool
	ActiveOperations  int64
	MaintenanceReason string
	ReadOnlyMode      bool
	RemainingTime     time.Duration
}

func (om *OperationManager) GetStatus() OperationStatus {
	maintStatus := om.maintenanceManager.GetMaintenanceStatus()
	return OperationStatus{
		IsMaintenanceMode: maintStatus.IsMaintenanceMode,
		IsShuttingDown:    om.shutdownManager.IsShuttingDown(),
		ActiveOperations:  om.shutdownManager.GetOperationCount(),
		MaintenanceReason: maintStatus.Reason,
		ReadOnlyMode:      maintStatus.ReadOnlyMode,
		RemainingTime:     maintStatus.RemainingTime,
	}
}
