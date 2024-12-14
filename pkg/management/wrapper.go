package management

import (
	"context"
	"fmt"
	"time"

	"redis-management/pkg/errors"
	"redis-management/pkg/logging"
)

// OperationWrapper provides common functionality for Redis operations
type OperationWrapper struct {
	service *RedisService
	logger  logging.Logger
}

// NewOperationWrapper creates a new operation wrapper
func NewOperationWrapper(service *RedisService, logger logging.Logger) *OperationWrapper {
	return &OperationWrapper{
		service: service,
		logger:  logger,
	}
}

// operationContext holds context for a single operation
type operationContext struct {
	ctx       context.Context
	operation string
	fields    map[string]interface{}
	start     time.Time
}

// ResultType is an interface for operation results
type ResultType interface {
	string | int64 | float64 | map[string]string | map[string]time.Duration
}

// WrapOperation wraps a Redis operation with common functionality
func (w *OperationWrapper) WrapOperation(ctx context.Context, operation string, fields map[string]interface{}, fn func() error) error {
	opCtx := &operationContext{
		ctx:       ctx,
		operation: operation,
		fields:    fields,
		start:     time.Now(),
	}

	// Create error builder
	errBuilder := errors.NewErrorBuilderFromContext(ctx).
		WithOperation(operation).
		WithFields(fields)

	// Initialize logger for this operation
	logger := w.logger.
		WithContext(ctx).
		WithField("operation", operation).
		WithFields(fields)

	// Log operation start
	logger.Debug("starting operation")

	// Execute operation
	err := w.executeOperation(opCtx, fn)

	// Handle operation result
	return w.handleOperationResult(opCtx, err, errBuilder, logger)
}

// WrapOperationWithResult wraps a Redis operation that returns a result
func (w *OperationWrapper) WrapOperationWithResult(ctx context.Context, operation string, fields map[string]interface{}, fn func() (interface{}, error)) (interface{}, error) {
	opCtx := &operationContext{
		ctx:       ctx,
		operation: operation,
		fields:    fields,
		start:     time.Now(),
	}

	// Create error builder
	errBuilder := errors.NewErrorBuilderFromContext(ctx).
		WithOperation(operation).
		WithFields(fields)

	// Initialize logger for this operation
	logger := w.logger.
		WithContext(ctx).
		WithField("operation", operation).
		WithFields(fields)

	// Log operation start
	logger.Debug("starting operation")

	// Execute operation
	result, err := w.executeOperationWithResult(opCtx, fn)

	// Handle operation result
	if err = w.handleOperationResult(opCtx, err, errBuilder, logger); err != nil {
		return nil, err
	}

	return result, nil
}

// executeOperation executes the operation with common checks
func (w *OperationWrapper) executeOperation(opCtx *operationContext, fn func() error) error {
	// Check for context cancellation
	if err := opCtx.ctx.Err(); err != nil {
		return err
	}

	// Check maintenance mode
	if !w.service.operationManager.GetMaintenanceManager().IsOperationAllowed(opCtx.operation) {
		return fmt.Errorf("operation not allowed during maintenance mode")
	}

	// Track operation for shutdown
	if !w.service.operationManager.GetShutdownManager().TrackOperation(opCtx.ctx) {
		return fmt.Errorf("service is shutting down")
	}
	defer w.service.operationManager.GetShutdownManager().FinishOperation()

	// Execute the operation
	return fn()
}

// executeOperationWithResult executes the operation that returns a result
func (w *OperationWrapper) executeOperationWithResult(opCtx *operationContext, fn func() (interface{}, error)) (interface{}, error) {
	// Check for context cancellation
	if err := opCtx.ctx.Err(); err != nil {
		return nil, err
	}

	// Check maintenance mode
	if !w.service.operationManager.GetMaintenanceManager().IsOperationAllowed(opCtx.operation) {
		return nil, fmt.Errorf("operation not allowed during maintenance mode")
	}

	// Track operation for shutdown
	if !w.service.operationManager.GetShutdownManager().TrackOperation(opCtx.ctx) {
		return nil, fmt.Errorf("service is shutting down")
	}
	defer w.service.operationManager.GetShutdownManager().FinishOperation()

	// Execute the operation
	return fn()
}

// handleOperationResult processes the operation result and handles logging
func (w *OperationWrapper) handleOperationResult(opCtx *operationContext, err error, errBuilder *errors.ErrorBuilder, logger logging.Logger) error {
	duration := time.Since(opCtx.start)
	logger = logger.WithField("duration_ms", float64(duration)/float64(time.Millisecond))

	if err != nil {
		// Convert to RedisError if it isn't already
		var redisErr *errors.RedisError
		if e, ok := err.(*errors.RedisError); ok {
			redisErr = e
		} else {
			redisErr = errBuilder.InvalidOperation(
				err,
				fmt.Sprintf("operation %s failed", opCtx.operation),
			)
		}

		// Log error with context
		logger.WithError(redisErr).Error("operation failed")

		// Update metrics
		w.service.operationManager.UpdateMetrics(duration, false)

		return redisErr
	}

	// Log success
	logger.Info("operation completed successfully")

	// Update metrics
	w.service.operationManager.UpdateMetrics(duration, true)

	return nil
}

// Common wrapper methods for different operation types

// WrapGet wraps a GET operation
func (w *OperationWrapper) WrapGet(ctx context.Context, key string, fn func() (string, error)) (string, error) {
	fields := map[string]interface{}{
		"key": key,
	}

	// Create a wrapper function that converts string to interface{}
	wrappedFn := func() (interface{}, error) {
		return fn()
	}

	result, err := w.WrapOperationWithResult(ctx, "GET", fields, wrappedFn)
	if err != nil {
		return "", err
	}

	// Add type assertion
	if strResult, ok := result.(string); ok {
		return strResult, nil
	}
	return "", fmt.Errorf("unexpected result type from operation")
}

// WrapSet wraps a SET operation
func (w *OperationWrapper) WrapSet(ctx context.Context, key string, value interface{}, fn func() error) error {
	fields := map[string]interface{}{
		"key":        key,
		"value_type": fmt.Sprintf("%T", value),
	}
	return w.WrapOperation(ctx, "SET", fields, fn)
}

// WrapDelete wraps a DELETE operation
func (w *OperationWrapper) WrapDelete(ctx context.Context, key string, fn func() error) error {
	fields := map[string]interface{}{
		"key": key,
	}
	return w.WrapOperation(ctx, "DELETE", fields, fn)
}

// WrapBatch wraps a batch operation
func (w *OperationWrapper) WrapBatch(ctx context.Context, operation string, keys []string, fn func() error) error {
	fields := map[string]interface{}{
		"keys_count": len(keys),
		"keys":       keys,
	}
	return w.WrapOperation(ctx, fmt.Sprintf("BATCH_%s", operation), fields, fn)
}
