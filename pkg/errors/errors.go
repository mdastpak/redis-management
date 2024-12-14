package errors

import (
	"encoding/json"
	"fmt"
	"time"
)

// RedisError represents a structured error with additional context
type RedisError struct {
	// Basic error information
	Code    ErrorCode              `json:"code"`
	Message string                 `json:"message"`
	Err     error                  `json:"-"` // Original error
	Fields  map[string]interface{} `json:"fields,omitempty"`

	// Additional context
	Timestamp time.Time `json:"timestamp"`
	TraceID   string    `json:"trace_id,omitempty"`
	Operation string    `json:"operation,omitempty"`

	// Retry information
	RetryCount int  `json:"retry_count,omitempty"`
	Retryable  bool `json:"retryable"`
}

// Error implements the error interface
func (e *RedisError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s-%d] %s: %v", e.Code.Category(), e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s-%d] %s", e.Code.Category(), e.Code, e.Message)
}

// Unwrap returns the underlying error
func (e *RedisError) Unwrap() error {
	return e.Err
}

// WithField adds a single field to the error context
func (e *RedisError) WithField(key string, value interface{}) *RedisError {
	if e.Fields == nil {
		e.Fields = make(map[string]interface{})
	}
	e.Fields[key] = value
	return e
}

// WithFields adds multiple fields to the error context
func (e *RedisError) WithFields(fields map[string]interface{}) *RedisError {
	if e.Fields == nil {
		e.Fields = make(map[string]interface{})
	}
	for k, v := range fields {
		e.Fields[k] = v
	}
	return e
}

// WithOperation sets the operation name for the error
func (e *RedisError) WithOperation(op string) *RedisError {
	e.Operation = op
	return e
}

// WithTraceID sets the trace ID for the error
func (e *RedisError) WithTraceID(traceID string) *RedisError {
	e.TraceID = traceID
	return e
}

// WithRetryCount sets the retry count for the error
func (e *RedisError) WithRetryCount(count int) *RedisError {
	e.RetryCount = count
	return e
}

// IsRetryable returns whether the error is retryable
func (e *RedisError) IsRetryable() bool {
	return e.Retryable || e.Code.IsRetryable()
}

// MarshalJSON implements json.Marshaler interface
func (e *RedisError) MarshalJSON() ([]byte, error) {
	type Alias RedisError
	return json.Marshal(&struct {
		*Alias
		ErrorString string `json:"error"`
	}{
		Alias:       (*Alias)(e),
		ErrorString: e.Error(),
	})
}

// New creates a new RedisError
func New(code ErrorCode, msg string, err error) *RedisError {
	return &RedisError{
		Code:      code,
		Message:   msg,
		Err:       err,
		Timestamp: time.Now(),
		Retryable: code.IsRetryable(),
	}
}

// Wrap wraps an existing error with additional context
func Wrap(err error, code ErrorCode, msg string) *RedisError {
	if err == nil {
		return nil
	}

	// If it's already a RedisError, update it
	if redisErr, ok := err.(*RedisError); ok {
		redisErr.Code = code
		redisErr.Message = msg
		return redisErr
	}

	return New(code, msg, err)
}

// Is implements error matching for RedisError
func (e *RedisError) Is(target error) bool {
	t, ok := target.(*RedisError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// AsRedisError attempts to convert an error to RedisError
func AsRedisError(err error) (*RedisError, bool) {
	if err == nil {
		return nil, false
	}

	var redisErr *RedisError
	if ok := As(err, &redisErr); ok {
		return redisErr, true
	}
	return nil, false
}

// As implements error unwrapping for RedisError
func As(err error, target interface{}) bool {
	if target == nil {
		return false
	}

	// Check if the error is already a RedisError
	if redisErr, ok := err.(*RedisError); ok {
		switch t := target.(type) {
		case **RedisError:
			*t = redisErr
			return true
		}
	}

	return false
}
