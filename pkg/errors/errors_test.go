package errors

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisError(t *testing.T) {
	t.Parallel()
	t.Run("Basic Error Creation", func(t *testing.T) {
		err := New(ErrDataNotFound, "test message", nil)
		assert.Equal(t, ErrDataNotFound, err.Code)
		assert.Equal(t, "test message", err.Message)
		assert.Contains(t, err.Error(), "test message")
		assert.Contains(t, err.Error(), "Data")
	})

	t.Run("Error with Fields", func(t *testing.T) {
		err := New(ErrKeyInvalid, "invalid key", nil).
			WithField("key", "test_key").
			WithField("reason", "too_long")

		assert.Equal(t, "test_key", err.Fields["key"])
		assert.Equal(t, "too_long", err.Fields["reason"])
	})

	t.Run("Error Wrapping", func(t *testing.T) {
		originalErr := fmt.Errorf("original error")
		wrappedErr := Wrap(originalErr, ErrSystemInternal, "system error")

		assert.Equal(t, ErrSystemInternal, wrappedErr.Code)
		assert.Equal(t, "system error", wrappedErr.Message)
		assert.Equal(t, originalErr, wrappedErr.Unwrap())
	})

	t.Run("Error Categories", func(t *testing.T) {
		assert.Equal(t, "System", ErrSystemInternal.Category())
		assert.Equal(t, "Data", ErrDataNotFound.Category())
		assert.Equal(t, "Operation", ErrOperationInvalid.Category())
	})

	t.Run("Retryable Errors", func(t *testing.T) {
		assert.True(t, ErrConnectionTimeout.IsRetryable())
		assert.False(t, ErrDataNotFound.IsRetryable())
	})

	t.Run("Error JSON Marshaling", func(t *testing.T) {
		err := New(ErrKeyInvalid, "invalid key", nil).
			WithField("key", "test_key")

		data, marshalErr := err.MarshalJSON()
		require.NoError(t, marshalErr)

		assert.Contains(t, string(data), "invalid key")
		assert.Contains(t, string(data), "test_key")
	})
}

func TestErrorBuilder(t *testing.T) {
	t.Parallel()
	t.Run("Basic Builder Usage", func(t *testing.T) {
		builder := NewErrorBuilder()
		err := builder.WithOperation("SET").
			WithField("key", "test_key").
			Build(ErrKeyInvalid, "invalid key format", nil)

		assert.Equal(t, ErrKeyInvalid, err.Code)
		assert.Equal(t, "SET", err.Operation)
		assert.Equal(t, "test_key", err.Fields["key"])
	})

	t.Run("Builder with Context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), TraceIDKey, "trace-123")
		builder := NewErrorBuilderFromContext(ctx)
		err := builder.Build(ErrDataNotFound, "key not found", nil)

		assert.Equal(t, "trace-123", err.TraceID)
	})

	t.Run("Common Error Types", func(t *testing.T) {
		builder := NewErrorBuilder()

		// Test system error
		sysErr := builder.System(nil, "system error")
		assert.Equal(t, ErrSystemInternal, sysErr.Code)

		// Test connection error
		connErr := builder.Connection(nil, "connection failed")
		assert.Equal(t, ErrConnectionFailed, connErr.Code)

		// Test validation error
		// valErr := builder.Validation("invalid input", nil, map[string]interface{}{"field": "test"})
		// assert.Equal(t, ErrDataInvalid, valErr.Code)
		// assert.Equal(t, "test", valErr.Fields["field"])
	})

	t.Run("Error Field Management", func(t *testing.T) {
		builder := NewErrorBuilder()
		err := builder.
			WithField("key1", "value1").
			WithFields(map[string]interface{}{
				"key2": "value2",
				"key3": 123,
			}).
			Build(ErrOperationInvalid, "operation failed", nil)

		assert.Equal(t, "value1", err.Fields["key1"])
		assert.Equal(t, "value2", err.Fields["key2"])
		assert.Equal(t, 123, err.Fields["key3"])
	})
}

func TestErrorCodeBehavior(t *testing.T) {
	t.Parallel()
	t.Run("Code Ranges", func(t *testing.T) {
		// System errors should be in 1000-1099 range
		assert.True(t, ErrSystemInternal >= 1000 && ErrSystemInternal < 1100)

		// Connection errors should be in 1100-1199 range
		assert.True(t, ErrConnectionFailed >= 1100 && ErrConnectionFailed < 1200)

		// Operation errors should be in 1200-1299 range
		assert.True(t, ErrOperationInvalid >= 1200 && ErrOperationInvalid < 1300)
	})

	t.Run("Code String Representation", func(t *testing.T) {
		assert.NotEqual(t, "unknown error code", ErrSystemInternal.String())
		assert.NotEqual(t, "unknown error code", ErrConnectionFailed.String())
		assert.Contains(t, ErrDataNotFound.String(), "not found")
	})
}
