package management

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type MaintenanceManager struct {
	service *RedisService
	status  struct {
		isMaintenanceMode bool
		startTime         time.Time
		endTime           time.Time
		reason            string
		readOnlyMode      bool
	}
	mu sync.RWMutex
}

func NewMaintenanceManager(service *RedisService) *MaintenanceManager {
	return &MaintenanceManager{
		service: service,
	}
}

// EnableMaintenance activates maintenance mode
func (mm *MaintenanceManager) EnableMaintenance(ctx context.Context, duration time.Duration, reason string, readOnly bool) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if mm.status.isMaintenanceMode {
		return fmt.Errorf("maintenance mode already active")
	}

	mm.status.isMaintenanceMode = true
	mm.status.startTime = time.Now()
	mm.status.endTime = mm.status.startTime.Add(duration)
	mm.status.reason = reason
	mm.status.readOnlyMode = readOnly

	// Schedule automatic maintenance mode disable
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(duration):
			mm.DisableMaintenance()
		}
	}()

	return nil
}

// DisableMaintenance deactivates maintenance mode
func (mm *MaintenanceManager) DisableMaintenance() error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if !mm.status.isMaintenanceMode {
		return fmt.Errorf("maintenance mode not active")
	}

	mm.status.isMaintenanceMode = false
	mm.status.readOnlyMode = false
	return nil
}

// IsOperationAllowed checks if an operation is allowed during maintenance
func (mm *MaintenanceManager) IsOperationAllowed(operation string) bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if !mm.status.isMaintenanceMode {
		return true
	}

	if !mm.status.readOnlyMode {
		return false
	}

	// Allow read operations during read-only maintenance
	readOnlyOps := map[string]bool{
		"GET":    true,
		"MGET":   true,
		"EXISTS": true,
		"TTL":    true,
		"PING":   true,
	}

	return readOnlyOps[operation]
}

// GetMaintenanceStatus returns current maintenance status
func (mm *MaintenanceManager) GetMaintenanceStatus() MaintenanceStatus {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	return MaintenanceStatus{
		IsMaintenanceMode: mm.status.isMaintenanceMode,
		StartTime:         mm.status.startTime,
		EndTime:           mm.status.endTime,
		Reason:            mm.status.reason,
		ReadOnlyMode:      mm.status.readOnlyMode,
		RemainingTime:     time.Until(mm.status.endTime),
	}
}

type MaintenanceStatus struct {
	IsMaintenanceMode bool
	StartTime         time.Time
	EndTime           time.Time
	Reason            string
	ReadOnlyMode      bool
	RemainingTime     time.Duration
}
