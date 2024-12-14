package errors

import (
	"context"
	"fmt"
)

// ErrorBuilder provides a fluent interface for building RedisErrors
type ErrorBuilder struct {
	traceID   string
	operation string
	fields    map[string]interface{}
}

// NewErrorBuilder creates a new ErrorBuilder instance
func NewErrorBuilder() *ErrorBuilder {
	return &ErrorBuilder{
		fields: make(map[string]interface{}),
	}
}

// NewErrorBuilderFromContext creates a new ErrorBuilder and extracts trace ID from context
func NewErrorBuilderFromContext(ctx context.Context) *ErrorBuilder {
	eb := NewErrorBuilder()
	if ctx != nil {
		if traceID, ok := ctx.Value(TraceIDKey).(string); ok {
			eb.traceID = traceID
		}
	}
	return eb
}

// WithTraceID sets the trace ID
func (eb *ErrorBuilder) WithTraceID(traceID string) *ErrorBuilder {
	eb.traceID = traceID
	return eb
}

// WithOperation sets the operation name
func (eb *ErrorBuilder) WithOperation(op string) *ErrorBuilder {
	eb.operation = op
	return eb
}

// WithField adds a single field
func (eb *ErrorBuilder) WithField(key string, value interface{}) *ErrorBuilder {
	eb.fields[key] = value
	return eb
}

// WithFields adds multiple fields
func (eb *ErrorBuilder) WithFields(fields map[string]interface{}) *ErrorBuilder {
	for k, v := range fields {
		eb.fields[k] = v
	}
	return eb
}

// Build creates a new RedisError with the configured options
func (eb *ErrorBuilder) Build(code ErrorCode, msg string, err error) *RedisError {
	redisErr := New(code, msg, err)
	redisErr.TraceID = eb.traceID
	redisErr.Operation = eb.operation
	redisErr.Fields = eb.fields
	return redisErr
}

// Common error builders for frequently used error types
func (eb *ErrorBuilder) System(err error, msg string) *RedisError {
	return eb.Build(ErrSystemInternal, msg, err)
}

func (eb *ErrorBuilder) Config(err error, msg string) *RedisError {
	return eb.Build(ErrSystemConfig, msg, err)
}

func (eb *ErrorBuilder) Connection(err error, msg string) *RedisError {
	return eb.Build(ErrConnectionFailed, msg, err)
}

func (eb *ErrorBuilder) Timeout(err error, msg string) *RedisError {
	return eb.Build(ErrOperationTimeout, msg, err)
}

func (eb *ErrorBuilder) NotFound(err error, msg string) *RedisError {
	return eb.Build(ErrDataNotFound, msg, err)
}

func (eb *ErrorBuilder) InvalidKey(err error, msg string) *RedisError {
	return eb.Build(ErrKeyInvalid, msg, err)
}

func (eb *ErrorBuilder) InvalidOperation(err error, msg string) *RedisError {
	return eb.Build(ErrOperationInvalid, msg, err)
}

func (eb *ErrorBuilder) MaintenanceMode(err error, msg string) *RedisError {
	return eb.Build(ErrSystemMaintenance, msg, err)
}

func (eb *ErrorBuilder) BulkOperation(err error, msg string) *RedisError {
	return eb.Build(ErrBulkFailed, msg, err)
}

// Helper functions for common error messages
func (eb *ErrorBuilder) KeyNotFound(key string) *RedisError {
	return eb.WithField("key", key).
		Build(ErrKeyNotFound, fmt.Sprintf("key not found: %s", key), nil)
}

func (eb *ErrorBuilder) InvalidKeyFormat(key string, reason string) *RedisError {
	return eb.WithField("key", key).
		WithField("reason", reason).
		Build(ErrKeyInvalid, fmt.Sprintf("invalid key format: %s", reason), nil)
}

func (eb *ErrorBuilder) OperationNotAllowed(operation string, reason string) *RedisError {
	return eb.WithField("operation", operation).
		WithField("reason", reason).
		Build(ErrOperationNotAllowed, fmt.Sprintf("operation not allowed: %s", reason), nil)
}

func (eb *ErrorBuilder) ConnectionError(err error) *RedisError {
	return eb.Build(ErrConnectionFailed, "connection failed", err)
}

// Context key for trace ID
type contextKey string

const (
	// TraceIDKey is the context key for trace ID
	TraceIDKey contextKey = "trace_id"
)

// Example usage:
/*
func ExampleUsage() {
	ctx := context.Background()

	// Simple usage
	err := NewErrorBuilder().
		WithOperation("SET").
		WithField("key", "mykey").
		System(nil, "system error occurred")

	// Context-aware usage
	err = NewErrorBuilderFromContext(ctx).
		WithOperation("GET").
		WithField("key", "mykey").
		KeyNotFound("mykey")

	// Chained usage with common error
	err = NewErrorBuilder().
		WithOperation("BULK_SET").
		WithFields(map[string]interface{}{
			"batch_size": 100,
			"failed_keys": []string{"key1", "key2"},
		}).
		BulkOperation(nil, "bulk operation partially failed")
}
*/
