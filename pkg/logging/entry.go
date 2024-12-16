package logging

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"redis-management/pkg/errors"
)

// Entry represents a log entry with all its fields and metadata
type Entry struct {
	// Basic fields
	Level     Level                  `json:"level"`
	Message   string                 `json:"message"`
	Timestamp time.Time              `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields,omitempty"`

	// Error information
	RedisError *errors.RedisError `json:"error,omitempty"`
	ErrorText  string             `json:"error_text,omitempty"`

	// Context information
	TraceID    string `json:"trace_id,omitempty"`
	Operation  string `json:"operation,omitempty"`
	Component  string `json:"component,omitempty"`
	CallerFunc string `json:"caller_func,omitempty"`
	CallerFile string `json:"caller_file,omitempty"`
	CallerLine int    `json:"caller_line,omitempty"`

	// Performance metrics
	Duration time.Duration `json:"duration,omitempty"`

	// Internal fields
	logger Logger
}

// NewEntry creates a new log entry
func NewEntry(logger Logger) *Entry {
	return &Entry{
		logger:    logger,
		Fields:    make(map[string]interface{}),
		Timestamp: time.Now(),
	}
}

// WithError adds an error to the entry
func (e *Entry) WithError(err error) *Entry {
	if err == nil {
		return e
	}

	// If it's a RedisError, use it directly
	if redisErr, ok := err.(*errors.RedisError); ok {
		e.RedisError = redisErr
		e.ErrorText = redisErr.Error()
		// Add error fields to entry fields
		for k, v := range redisErr.Fields {
			e.WithField(k, v)
		}
		return e
	}

	// Convert regular error to string
	e.ErrorText = err.Error()
	return e
}

// WithField adds a field to the entry
func (e *Entry) WithField(key string, value interface{}) *Entry {
	e.Fields[key] = value
	return e
}

// WithFields adds multiple fields to the entry
func (e *Entry) WithFields(fields map[string]interface{}) *Entry {
	for k, v := range fields {
		e.Fields[k] = v
	}
	return e
}

// WithTrace adds trace information to the entry
func (e *Entry) WithTrace(traceID string) *Entry {
	e.TraceID = traceID
	return e
}

// WithOperation adds operation information to the entry
func (e *Entry) WithOperation(operation string) *Entry {
	e.Operation = operation
	return e
}

// WithComponent adds component information to the entry
func (e *Entry) WithComponent(component string) *Entry {
	e.Component = component
	return e
}

// WithDuration adds duration information to the entry
func (e *Entry) WithDuration(duration time.Duration) *Entry {
	e.Duration = duration
	return e
}

// setCaller sets the caller information for the entry
func (e *Entry) setCaller(skip int) *Entry {
	if pc, file, line, ok := runtime.Caller(skip); ok {
		e.CallerFunc = runtime.FuncForPC(pc).Name()
		e.CallerFile = filepath.Base(file)
		e.CallerLine = line
	}
	return e
}

// log performs the actual logging
func (e *Entry) log(level Level, msg string) {
	e.Level = level
	e.Message = msg
	e.setCaller(3) // skip log, logger method, and user code
	e.logger.Log(e)
}

// Log methods for different levels
func (e *Entry) Trace(msg string) {
	e.log(TraceLevel, msg)
}

func (e *Entry) Debug(msg string) {
	e.log(DebugLevel, msg)
}

func (e *Entry) Info(msg string) {
	e.log(InfoLevel, msg)
}

func (e *Entry) Warn(msg string) {
	e.log(WarnLevel, msg)
}

func (e *Entry) Error(msg string) {
	e.log(ErrorLevel, msg)
}

func (e *Entry) Fatal(msg string) {
	e.log(FatalLevel, msg)
}

// Formatted logging methods
func (e *Entry) Tracef(format string, args ...interface{}) {
	e.Trace(fmt.Sprintf(format, args...))
}

func (e *Entry) Debugf(format string, args ...interface{}) {
	e.Debug(fmt.Sprintf(format, args...))
}

func (e *Entry) Infof(format string, args ...interface{}) {
	e.Info(fmt.Sprintf(format, args...))
}

func (e *Entry) Warnf(format string, args ...interface{}) {
	e.Warn(fmt.Sprintf(format, args...))
}

func (e *Entry) Errorf(format string, args ...interface{}) {
	e.Error(fmt.Sprintf(format, args...))
}

func (e *Entry) Fatalf(format string, args ...interface{}) {
	e.Fatal(fmt.Sprintf(format, args...))
}

// MarshalJSON implements json.Marshaler interface
func (e *Entry) MarshalJSON() ([]byte, error) {
	type Alias Entry
	return json.Marshal(&struct {
		*Alias
		Level string `json:"level"`
	}{
		Alias: (*Alias)(e),
		Level: e.Level.String(),
	})
}

// clone creates a copy of the entry
func (e *Entry) clone() *Entry {
	fields := make(map[string]interface{}, len(e.Fields))
	for k, v := range e.Fields {
		fields[k] = v
	}

	return &Entry{
		Level:      e.Level,
		Message:    e.Message,
		Fields:     fields,
		Timestamp:  e.Timestamp,
		logger:     e.logger,
		TraceID:    e.TraceID,
		Operation:  e.Operation,
		Component:  e.Component,
		RedisError: e.RedisError,
		ErrorText:  e.ErrorText,
		Duration:   e.Duration,
	}
}
