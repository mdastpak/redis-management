// pkg/management/redis_reload.go

package management

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mdastpak/redis-management/config"

	"github.com/go-redis/redis/v8"
)

// Reload state tracks which components need reinitialization
type reloadState struct {
	needsPoolReload      bool
	needsKeyMgrReload    bool
	needsBulkReload      bool
	needsCircuitReload   bool
	needsOperationReload bool
}

// ReloadConfig updates service configuration and reinitializes components
func (rs *RedisService) ReloadConfig(newCfg *config.Config) error {
	if newCfg == nil {
		return fmt.Errorf("new configuration cannot be nil")
	}

	// Use existing validation from config package
	if err := config.ValidateConfig(newCfg); err != nil {
		return fmt.Errorf("configuration validation failed: %v", err)
	}

	// Create context with timeout for reload operation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Acquire write lock for service reconfiguration
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Store old configuration for possible rollback
	oldCfg := *rs.cfg // Create a copy of the old configuration

	// Determine what needs to be reloaded
	state := determineReloadState(&oldCfg, newCfg)
	// log.Printf("Detected configuration changes: %v", state)

	// If no changes detected, return early
	if !state.hasChanges() {
		log.Printf("No configuration changes detected, skipping reload")
		return nil
	}

	// Track components that need reinitialization
	var rollbackActions []func()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic during reload, rolling back: %v", r)
			executeRollback(rollbackActions)
			panic(r) // Re-panic after rollback
		}
	}()

	// Update base configuration
	*rs.cfg = *newCfg

	// Now reload methods can access both old and new configurations
	if err := rs.reloadComponents(ctx, state, &rollbackActions, &oldCfg); err != nil {
		log.Printf("Error during reload, rolling back: %v", err)
		executeRollback(rollbackActions)
		*rs.cfg = oldCfg
		return fmt.Errorf("reload failed: %v", err)
	}

	log.Printf("Service reconfiguration completed successfully")
	return nil
}

func determineReloadState(old, new *config.Config) reloadState {
	return reloadState{
		needsPoolReload:      poolConfigChanged(old.Pool, new.Pool),
		needsKeyMgrReload:    keyManagerConfigChanged(old.Redis, new.Redis),
		needsBulkReload:      bulkConfigChanged(old.Bulk, new.Bulk),
		needsCircuitReload:   circuitConfigChanged(old.Circuit, new.Circuit),
		needsOperationReload: operationConfigChanged(old, new),
	}
}

func (rs *RedisService) reloadComponents(ctx context.Context, state reloadState, rollbackActions *[]func(), oldCfg *config.Config) error {
	// Reload components in the correct order
	if state.needsKeyMgrReload {
		if err := rs.reloadKeyManager(rollbackActions); err != nil {
			return err
		}
	}

	if state.needsPoolReload {
		if err := rs.reloadPool(rollbackActions, &oldCfg.Pool); err != nil {
			return err
		}
	}

	// Reload secondary components in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, 3)

	if state.needsBulkReload {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rs.reloadBulkProcessor(rollbackActions, &oldCfg.Bulk); err != nil {
				errChan <- err
			}
		}()
	}

	if state.needsCircuitReload {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rs.reloadCircuitBreaker(rollbackActions, &oldCfg.Circuit); err != nil {
				errChan <- err
			}
		}()
	}

	if state.needsOperationReload {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rs.reloadOperationManager(rollbackActions); err != nil {
				errChan <- err
			}
		}()
	}

	// Wait with context timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("reload operation cancelled: %v", ctx.Err())
	case <-done:
		select {
		case err := <-errChan:
			return err
		default:
			return nil
		}
	}
}

func (rs *RedisService) reloadKeyManager(rollbackActions *[]func()) error {
	oldKeyMgr := rs.keyMgr
	newKeyMgr, err := NewKeyManager(rs.cfg)
	if err != nil {
		return fmt.Errorf("key manager reload failed: %v", err)
	}
	rs.keyMgr = newKeyMgr
	*rollbackActions = append(*rollbackActions, func() {
		rs.keyMgr = oldKeyMgr
	})
	return nil
}

// func (rs *RedisService) reloadPool(rollbackActions *[]func()) error {

func (rs *RedisService) reloadPool(rollbackActions *[]func(), oldConfig *config.PoolConfig) error {
	// No action needed if both configurations are disabled
	if !oldConfig.Status && !rs.cfg.Pool.Status {
		return nil
	}

	// Store current state for possible rollback
	oldPool := rs.pool
	oldPoolManager := rs.poolManager

	// Clean up old pool if it was enabled
	if oldConfig.Status {
		cleanupPool(oldPoolManager, oldPool)
		rs.pool = nil
		rs.poolManager = nil
	}

	// Initialize new pool if enabled in new configuration
	if rs.cfg.Pool.Status {

		// Create new pool manager
		if err := rs.NewPoolManager(context.Background()); err != nil {
			// Restore old state on failure
			rs.pool = oldPool
			rs.poolManager = oldPoolManager
			return fmt.Errorf("pool manager creation failed: %v", err)
		}

		// Add rollback action
		*rollbackActions = append(*rollbackActions, func() {
			// Clean up new pool if it was created
			cleanupPool(rs.poolManager, rs.pool)
			rs.pool = oldPool
			rs.poolManager = oldPoolManager
		})
	}

	return nil
}

// Helper function to clean up pool resources
func cleanupPool(manager *PoolManager, pool *redis.Client) {
	if manager != nil {
		manager.Stop()
	}
	if pool != nil {
		pool.Close()
	}
}

func (rs *RedisService) reloadBulkProcessor(rollbackActions *[]func(), oldConfig *config.BulkConfig) error {
	// No action needed if both configurations are disabled
	if !oldConfig.Status && !rs.cfg.Bulk.Status {
		return nil
	}

	// Store current state for possible rollback
	oldBulkQueue := rs.bulkQueue

	// Clean up old bulk processor if it was enabled
	if oldConfig.Status {
		if oldBulkQueue != nil {
			close(oldBulkQueue)
			rs.bulkQueue = nil
		}
	}

	// Initialize new bulk processor if enabled in new configuration
	if rs.cfg.Bulk.Status {
		newBulkQueue := make(chan BulkOperation, rs.cfg.Bulk.BatchSize)
		rs.bulkQueue = newBulkQueue

		// Start new bulk processor
		processor := NewBulkProcessor(rs)
		go processor.Start(context.Background())

		// Add rollback action
		*rollbackActions = append(*rollbackActions, func() {
			if rs.bulkQueue != nil {
				close(rs.bulkQueue)
			}
			rs.bulkQueue = oldBulkQueue
		})
	}

	return nil
}

func (rs *RedisService) reloadCircuitBreaker(rollbackActions *[]func(), oldConfig *config.CircuitConfig) error {
	// No action needed if both configurations are disabled
	if !oldConfig.Status && !rs.cfg.Circuit.Status {
		return nil
	}

	// Store current state for possible rollback
	oldCB := rs.cb

	// Clean up old circuit breaker if it was enabled
	if oldConfig.Status {
		rs.cb = nil
	}

	// Initialize new circuit breaker if enabled in new configuration
	if rs.cfg.Circuit.Status {
		rs.cb = NewCircuitBreaker(
			rs.cfg.Circuit.Threshold,
			time.Duration(rs.cfg.Circuit.ResetTimeout)*time.Second,
			rs.cfg.Circuit.MaxHalfOpen,
		)

		// Add rollback action
		*rollbackActions = append(*rollbackActions, func() {
			rs.cb = oldCB
		})
	}

	return nil
}

func (rs *RedisService) reloadOperationManager(rollbackActions *[]func()) error {
	oldOpManager := rs.operationManager

	// First check if there are any relevant configuration changes
	if operationConfigChanged(rs.cfg, rs.cfg) {

		// Create new operation manager with updated configuration
		newOpManager, err := NewOperationManager(rs)
		// Initialize and verify the new manager
		if err != nil {
			// Rollback to old manager if initialization fails
			rs.operationManager = oldOpManager
			return fmt.Errorf("operation manager initialization failed: %v", err)
		}

		// Properly shutdown the old manager if it exists
		if oldOpManager != nil {
			if err := oldOpManager.GetShutdownManager().Shutdown(context.Background()); err != nil {
				return fmt.Errorf("failed to shutdown old operation manager: %v", err)
			}
		}

		rs.operationManager = newOpManager

		// Setup rollback action
		*rollbackActions = append(*rollbackActions, func() {
			if rs.operationManager != nil && rs.operationManager != oldOpManager {
				// Shutdown new manager before rolling back
				_ = rs.operationManager.GetShutdownManager().Shutdown(context.Background())
			}
			rs.operationManager = oldOpManager
		})
	}

	return nil
}

// Helper method for operation manager
func (om *OperationManager) Verify() error {
	// Basic verification
	if om.service == nil {
		return fmt.Errorf("operation manager service is nil")
	}
	if om.maintenanceManager == nil {
		return fmt.Errorf("maintenance manager is nil")
	}
	if om.shutdownManager == nil {
		return fmt.Errorf("shutdown manager is nil")
	}
	return nil
}

func executeRollback(actions []func()) {
	for i := len(actions) - 1; i >= 0; i-- {
		actions[i]()
	}
}

// Helper functions to determine what needs to be reloaded
func poolConfigChanged(old, new config.PoolConfig) bool {
	return old.Status != new.Status ||
		old.Size != new.Size ||
		old.MinIdle != new.MinIdle ||
		old.MaxIdleTime != new.MaxIdleTime ||
		old.WaitTimeout != new.WaitTimeout
}

func keyManagerConfigChanged(old, new config.RedisConfig) bool {
	return old.KeyPrefix != new.KeyPrefix ||
		old.HashKeys != new.HashKeys ||
		old.DB != new.DB
}

func bulkConfigChanged(old, new config.BulkConfig) bool {
	return old.Status != new.Status ||
		old.BatchSize != new.BatchSize ||
		old.FlushInterval != new.FlushInterval ||
		old.MaxRetries != new.MaxRetries ||
		old.ConcurrentFlush != new.ConcurrentFlush
}

func circuitConfigChanged(old, new config.CircuitConfig) bool {
	return old.Status != new.Status ||
		old.Threshold != new.Threshold ||
		old.ResetTimeout != new.ResetTimeout ||
		old.MaxHalfOpen != new.MaxHalfOpen
}

func operationConfigChanged(old, new *config.Config) bool {
	return old.Redis.ShutdownTimeout != new.Redis.ShutdownTimeout ||
		old.Redis.HealthCheckInterval != new.Redis.HealthCheckInterval ||
		old.Redis.RetryAttempts != new.Redis.RetryAttempts ||
		old.Redis.RetryDelay != new.Redis.RetryDelay
}

func (rs reloadState) hasChanges() bool {
	return rs.needsPoolReload ||
		rs.needsKeyMgrReload ||
		rs.needsBulkReload ||
		rs.needsCircuitReload ||
		rs.needsOperationReload
}

func (rs reloadState) String() string {
	return fmt.Sprintf("Pool: %v, KeyMgr: %v, Bulk: %v, Circuit: %v, Operation: %v",
		rs.needsPoolReload,
		rs.needsKeyMgrReload,
		rs.needsBulkReload,
		rs.needsCircuitReload,
		rs.needsOperationReload)
}
