package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"redis-management/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testBuffer struct {
	bytes.Buffer
	closeCalled bool
}

func (b *testBuffer) Close() error {
	b.closeCalled = true
	return nil
}

func TestLogger(t *testing.T) {
	t.Parallel()
	t.Run("Basic Logging", func(t *testing.T) {
		buf := &testBuffer{}
		// t.Logf("Buffer before logger creation: %+v", buf)

		logger := NewLogger(
			WithOutput(buf),
			WithLevel(InfoLevel),
			WithFormatter(&JSONFormatter{
				TimestampFormat:   time.RFC3339,
				DisableHTMLEscape: true,
			}),
		)
		// t.Logf("Logger after creation: %+v", logger)

		logger.Info("test message")
		output := buf.String()
		// t.Logf("Raw buffer content: %q", output)

		// Remove trailing newline if exists for JSON parsing
		output = strings.TrimSpace(output)
		// t.Logf("Trimmed buffer content: %q", output)

		var logEntry map[string]interface{}
		err := json.Unmarshal([]byte(output), &logEntry)
		if err != nil {
			// t.Logf("JSON unmarshal error: %v", err)
			// t.Logf("Output that failed to parse: %q", output)
			t.Fatal(err)
		}

		assert.Equal(t, "INFO", logEntry["level"])
		assert.Equal(t, "test message", logEntry["msg"])
		assert.NotEmpty(t, logEntry["time"])
	})

	t.Run("Log Levels", func(t *testing.T) {
		buf := &testBuffer{}
		logger := NewLogger(
			WithOutput(buf),
			WithLevel(InfoLevel),
			WithFormatter(NewFormatter(FormatJSON)),
		)

		// Debug shouldn't be logged
		logger.Debug("debug message")
		assert.Empty(t, buf.String())

		// Info should be logged
		buf.Reset()
		logger.Info("info message")
		assert.Contains(t, buf.String(), "info message")

		// Error should be logged
		buf.Reset()
		logger.Error("error message")
		assert.Contains(t, buf.String(), "error message")
	})

	t.Run("Logging with Context", func(t *testing.T) {
		buf := &testBuffer{}
		logger := NewLogger(
			WithOutput(buf),
			WithLevel(InfoLevel),
			WithFormatter(NewFormatter(FormatJSON)),
		)

		ctx := context.WithValue(context.Background(), TraceIDKey, "trace-123")
		logger.WithContext(ctx).Info("test message")

		var logEntry map[string]interface{}
		err := json.Unmarshal(buf.Bytes(), &logEntry)
		require.NoError(t, err)

		assert.Equal(t, "trace-123", logEntry["trace_id"])
	})

	t.Run("Logging with Fields", func(t *testing.T) {
		buf := &testBuffer{}
		logger := NewLogger(
			WithOutput(buf),
			WithLevel(InfoLevel),
			WithFormatter(NewFormatter(FormatJSON)),
		)

		logger.WithField("key", "value").
			WithFields(map[string]interface{}{
				"number": 123,
				"bool":   true,
			}).
			Info("test message")

		var logEntry map[string]interface{}
		err := json.Unmarshal(buf.Bytes(), &logEntry)
		require.NoError(t, err)

		assert.Equal(t, "value", logEntry["key"])
		assert.Equal(t, float64(123), logEntry["number"])
		assert.Equal(t, true, logEntry["bool"])
	})

	t.Run("Logging with Error", func(t *testing.T) {
		buf := &testBuffer{}
		logger := NewLogger(
			WithOutput(buf),
			WithLevel(InfoLevel),
			WithFormatter(NewFormatter(FormatJSON)),
		)

		redisErr := errors.New(errors.ErrKeyInvalid, "invalid key", nil).
			WithField("key", "test_key")

		logger.WithError(redisErr).Error("operation failed")
		output := buf.String()
		// t.Logf("Raw output: %q", output)

		var logEntry map[string]interface{}
		err := json.Unmarshal([]byte(strings.TrimSpace(output)), &logEntry)
		require.NoError(t, err)

		// t.Logf("Parsed log entry: %+v", logEntry)

		errorData, ok := logEntry["error"].(map[string]interface{})
		require.True(t, ok, "error field should be a map")

		assert.Equal(t, float64(errors.ErrKeyInvalid), errorData["code"])
		assert.Equal(t, "invalid key", errorData["message"])

		if fields, ok := errorData["fields"].(map[string]interface{}); ok {
			assert.Equal(t, "test_key", fields["key"])
		}
	})
}

func TestFormatters(t *testing.T) {
	t.Parallel()
	t.Run("JSON Formatter", func(t *testing.T) {
		buf := &testBuffer{}
		logger := NewLogger(
			WithOutput(buf),
			WithLevel(InfoLevel),
			WithFormatter(&JSONFormatter{
				TimestampFormat:   time.RFC3339,
				DisableHTMLEscape: true,
			}),
		)

		logger.WithField("key", "value").Info("test message")
		output := buf.String()
		// t.Logf("Raw output: %q", output)

		var logEntry map[string]interface{}
		err := json.Unmarshal([]byte(strings.TrimSpace(output)), &logEntry)
		require.NoError(t, err)
		assert.Equal(t, "test message", logEntry["msg"])
		assert.Equal(t, "value", logEntry["key"])
	})

	t.Run("Text Formatter", func(t *testing.T) {
		buf := &testBuffer{}
		logger := NewLogger(
			WithOutput(buf),
			WithLevel(InfoLevel),
			WithFormatter(NewFormatter(FormatText)),
		)

		logger.WithField("key", "value").Info("test message")

		logLine := buf.String()
		assert.Contains(t, logLine, "test message")
		assert.Contains(t, logLine, "key=value")
		assert.Contains(t, logLine, "INFO")
	})
}

func TestLoggerConfiguration(t *testing.T) {
	t.Parallel()
	t.Run("Level Configuration", func(t *testing.T) {
		testCases := []struct {
			level          Level
			shouldLogDebug bool
			shouldLogInfo  bool
			shouldLogError bool
		}{
			{DebugLevel, true, true, true},
			{InfoLevel, false, true, true},
			{ErrorLevel, false, false, true},
		}

		for _, tc := range testCases {
			buf := &testBuffer{}
			logger := NewLogger(
				WithOutput(buf),
				WithLevel(tc.level),
				WithFormatter(NewFormatter(FormatJSON)),
			)

			buf.Reset()
			logger.Debug("debug")
			hasDebug := strings.Contains(buf.String(), "debug")
			assert.Equal(t, tc.shouldLogDebug, hasDebug)

			buf.Reset()
			logger.Info("info")
			hasInfo := strings.Contains(buf.String(), "info")
			assert.Equal(t, tc.shouldLogInfo, hasInfo)

			buf.Reset()
			logger.Error("error")
			hasError := strings.Contains(buf.String(), "error")
			assert.Equal(t, tc.shouldLogError, hasError)
		}
	})
}
