package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mdastpak/redis-management/pkg/errors"
	"github.com/mdastpak/redis-management/pkg/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestLogEntry struct {
	Message string
	Fields  map[string]interface{}
}

type TestLogger struct {
	logs          *[]TestLogEntry
	currentFields map[string]interface{}
}

func (l *TestLogger) Log(entry *logging.Entry)                               {}
func (l *TestLogger) WithField(key string, value interface{}) logging.Logger { return l }
func (l *TestLogger) WithFields(fields map[string]interface{}) logging.Logger {
	newLogger := &TestLogger{
		logs:          l.logs,
		currentFields: fields,
	}
	return newLogger
}
func (l *TestLogger) WithError(err error) logging.Logger             { return l }
func (l *TestLogger) WithContext(ctx context.Context) logging.Logger { return l }
func (l *TestLogger) WithComponent(component string) logging.Logger  { return l }

func (l *TestLogger) Trace(msg string) {
	*l.logs = append(*l.logs, TestLogEntry{
		Message: msg,
		Fields:  l.currentFields,
	})
}
func (l *TestLogger) Debug(msg string) {
	*l.logs = append(*l.logs, TestLogEntry{
		Message: msg,
		Fields:  l.currentFields,
	})
}
func (l *TestLogger) Info(msg string) {
	*l.logs = append(*l.logs, TestLogEntry{
		Message: msg,
		Fields:  l.currentFields,
	})
}
func (l *TestLogger) Warn(msg string) {
	*l.logs = append(*l.logs, TestLogEntry{
		Message: msg,
		Fields:  l.currentFields,
	})
}
func (l *TestLogger) Error(msg string) {
	*l.logs = append(*l.logs, TestLogEntry{
		Message: msg,
		Fields:  l.currentFields,
	})
}
func (l *TestLogger) Fatal(msg string) {
	*l.logs = append(*l.logs, TestLogEntry{
		Message: msg,
		Fields:  l.currentFields,
	})
}

func (l *TestLogger) Tracef(format string, args ...interface{}) {
	*l.logs = append(*l.logs, TestLogEntry{Message: fmt.Sprintf(format, args...)})
}
func (l *TestLogger) Debugf(format string, args ...interface{}) {
	*l.logs = append(*l.logs, TestLogEntry{Message: fmt.Sprintf(format, args...)})
}
func (l *TestLogger) Infof(format string, args ...interface{}) {
	*l.logs = append(*l.logs, TestLogEntry{Message: fmt.Sprintf(format, args...)})
}
func (l *TestLogger) Warnf(format string, args ...interface{}) {
	*l.logs = append(*l.logs, TestLogEntry{Message: fmt.Sprintf(format, args...)})
}
func (l *TestLogger) Errorf(format string, args ...interface{}) {
	*l.logs = append(*l.logs, TestLogEntry{Message: fmt.Sprintf(format, args...)})
}
func (l *TestLogger) Fatalf(format string, args ...interface{}) {
	*l.logs = append(*l.logs, TestLogEntry{Message: fmt.Sprintf(format, args...)})
}

// Helper methods for tests
func (l *TestLogger) Logs() []string {
	result := make([]string, len(*l.logs))
	for i, entry := range *l.logs {
		result[i] = entry.Message
	}
	return result
}

func (l *TestLogger) GetLogs() []TestLogEntry {
	return *l.logs
}

func NewTestLogger() *TestLogger {
	logs := make([]TestLogEntry, 0)
	return &TestLogger{
		logs:          &logs,
		currentFields: make(map[string]interface{}),
	}
}

func setupTestWrapper(t *testing.T) (*OperationWrapper, *TestLogger) {
	testLogger := NewTestLogger()
	service, err := setupTestRedis(context.Background())
	require.NoError(t, err)

	wrapper := NewOperationWrapper(service, testLogger)
	return wrapper, testLogger
}

func TestOperationWrapper(t *testing.T) {
	t.Parallel()
	t.Run("Basic Operation Success", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		err := wrapper.WrapOperation(ctx, "TEST", map[string]interface{}{
			"test_field": "test_value",
		}, func() error {
			return nil
		})

		assert.NoError(t, err)
		logs := logger.Logs()
		assert.Contains(t, logs, "starting operation")
		assert.Contains(t, logs, "operation completed successfully")
	})

	t.Run("Operation With Error", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		expectedErr := fmt.Errorf("test error")
		err := wrapper.WrapOperation(ctx, "TEST", nil, func() error {
			return expectedErr
		})

		assert.Error(t, err)
		logs := logger.Logs()
		assert.Contains(t, logs, "operation failed")
		redisErr, ok := err.(*errors.RedisError)
		assert.True(t, ok)
		assert.Equal(t, "TEST", redisErr.Operation)
	})

	t.Run("Operation With Result Success", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		result, err := wrapper.WrapOperationWithResult(ctx, "TEST", nil, func() (interface{}, error) {
			return "test_result", nil
		})

		assert.NoError(t, err)
		assert.Equal(t, "test_result", result)
		logs := logger.Logs()
		assert.Contains(t, logs, "starting operation")
		assert.Contains(t, logs, "operation completed successfully")
	})

	t.Run("Operation With Result Error", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		expectedErr := fmt.Errorf("test error")
		result, err := wrapper.WrapOperationWithResult(ctx, "TEST", nil, func() (interface{}, error) {
			return nil, expectedErr
		})

		assert.Error(t, err)
		assert.Nil(t, result)
		logs := logger.Logs()
		assert.Contains(t, logs, "operation failed")
	})

	t.Run("Get Operation", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		result, err := wrapper.WrapGet(ctx, "test_key", func() (string, error) {
			return "test_value", nil
		})

		assert.NoError(t, err)
		assert.Equal(t, "test_value", result)
		logs := logger.Logs()
		assert.Contains(t, logs, "starting operation")
	})

	t.Run("Set Operation", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		err := wrapper.WrapSet(ctx, "test_key", "test_value", func() error {
			return nil
		})

		assert.NoError(t, err)
		logs := logger.Logs()
		assert.Contains(t, logs, "starting operation")
	})

	t.Run("Delete Operation", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		err := wrapper.WrapDelete(ctx, "test_key", func() error {
			return nil
		})

		assert.NoError(t, err)
		logs := logger.Logs()
		assert.Contains(t, logs, "starting operation")
	})

	t.Run("Batch Operations", func(t *testing.T) {
		wrapper, _ := setupTestWrapper(t)
		ctx := context.Background()

		t.Run("Batch Get", func(t *testing.T) {
			result, err := wrapper.WrapBatchGet(ctx, []string{"key1", "key2"}, func() (map[string]string, error) {
				return map[string]string{"key1": "value1", "key2": "value2"}, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, "value1", result["key1"])
			assert.Equal(t, "value2", result["key2"])
		})

		t.Run("Batch Set", func(t *testing.T) {
			items := map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			}
			err := wrapper.WrapBatchSet(ctx, items, func() error {
				return nil
			})

			assert.NoError(t, err)
		})
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		wrapper, _ := setupTestWrapper(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := wrapper.WrapOperation(ctx, "TEST", nil, func() error {
			return nil
		})

		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Operation Timing", func(t *testing.T) {
		wrapper, logger := setupTestWrapper(t)
		ctx := context.Background()

		sleepDuration := 100 * time.Millisecond
		err := wrapper.WrapOperation(ctx, "TEST", nil, func() error {
			time.Sleep(sleepDuration)
			return nil
		})

		assert.NoError(t, err)

		foundDuration := false
		for _, log := range *logger.logs {
			if _, ok := log.Fields["duration_ms"]; ok {
				foundDuration = true
				break
			}
		}
		assert.True(t, foundDuration, "Expected to find duration_ms in logs")
	})
}
