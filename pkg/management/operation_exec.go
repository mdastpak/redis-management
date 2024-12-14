package management

import (
	"context"
	"time"
)

// ExecuteWithLock executes an operation with proper locking and checks
func (om *OperationManager) ExecuteWithLock(ctx context.Context, cmd string, operation func() error) error {
	startTime := time.Now()

	if err := om.checkOperation(ctx, cmd); err != nil {
		om.UpdateMetrics(time.Since(startTime), false)
		return err
	}

	err := operation()
	om.UpdateMetrics(time.Since(startTime), err == nil)
	return err
}

// ExecuteReadOp executes a read operation that returns a string
func (om *OperationManager) ExecuteReadOp(ctx context.Context, cmd string, operation func() (string, error)) (string, error) {
	startTime := time.Now()

	if err := om.checkOperation(ctx, cmd); err != nil {
		om.UpdateMetrics(time.Since(startTime), false)
		return "", err
	}

	result, err := operation()
	om.UpdateMetrics(time.Since(startTime), err == nil)
	return result, err
}

// ExecuteDurationOp executes an operation that returns a time.Duration
func (om *OperationManager) ExecuteDurationOp(ctx context.Context, cmd string, operation func() (time.Duration, error)) (time.Duration, error) {
	startTime := time.Now()

	if err := om.checkOperation(ctx, cmd); err != nil {
		om.UpdateMetrics(time.Since(startTime), false)
		return 0, err
	}

	result, err := operation()
	om.UpdateMetrics(time.Since(startTime), err == nil)
	return result, err
}

// ExecuteBatchOp executes a batch operation that returns a string map
func (om *OperationManager) ExecuteBatchOp(ctx context.Context, cmd string, operation func() (map[string]string, error)) (map[string]string, error) {
	startTime := time.Now()

	if err := om.checkOperation(ctx, cmd); err != nil {
		om.UpdateMetrics(time.Since(startTime), false)
		return nil, err
	}

	result, err := operation()
	om.UpdateMetrics(time.Since(startTime), err == nil)
	return result, err
}

// ExecuteBatchDurationOp executes a batch operation that returns a duration map
func (om *OperationManager) ExecuteBatchDurationOp(ctx context.Context, cmd string, operation func() (map[string]time.Duration, error)) (map[string]time.Duration, error) {
	startTime := time.Now()

	if err := om.checkOperation(ctx, cmd); err != nil {
		om.UpdateMetrics(time.Since(startTime), false)
		return nil, err
	}

	result, err := operation()
	om.UpdateMetrics(time.Since(startTime), err == nil)
	return result, err
}
