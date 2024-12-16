package logging

import (
	"context"
	"fmt"
	"io"
	"os"
	"redis-management/pkg/errors"
	"sync"
	"time"
)

// Context key type
type contextKey string

const (
	// TraceIDKey is the context key for trace ID
	TraceIDKey contextKey = "trace_id"
)

// Logger defines the interface for logging operations
type Logger interface {
	Log(entry *Entry)

	WithField(key string, value interface{}) Logger
	WithFields(fields map[string]interface{}) Logger
	WithError(err error) Logger
	WithContext(ctx context.Context) Logger
	WithComponent(component string) Logger

	Trace(msg string)
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
	Fatal(msg string)

	Tracef(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

// logger implements the Logger interface
type logger struct {
	// Configuration
	level     Level
	formatter Formatter
	output    io.Writer

	// Shared fields and context
	fields    map[string]interface{}
	component string

	// Synchronization
	mu sync.Mutex

	// Hooks for additional processing
	hooks []Hook

	// Optional error handler
	errorHandler ErrorHandler
	lastError    *errors.RedisError
}

// Hook allows adding additional processing for log entries
type Hook interface {
	Fire(*Entry) error
	Levels() []Level
}

// ErrorHandler handles errors during logging
type ErrorHandler func(error)

// LoggerOption defines a function to configure the logger
type LoggerOption func(*logger)

// NewLogger creates a new logger with the given options
func NewLogger(options ...LoggerOption) Logger {
	l := &logger{
		level:     InfoLevel,                    // default level
		formatter: NewFormatter(FormatJSON),     // default formatter
		output:    os.Stdout,                    // default output
		fields:    make(map[string]interface{}), // initialize fields map
	}

	// Apply options
	for _, option := range options {
		option(l)
	}

	return l
}

// Configuration options
func WithLevel(level Level) LoggerOption {
	return func(l *logger) {
		l.level = level
	}
}

func WithFormatter(formatter Formatter) LoggerOption {
	return func(l *logger) {
		l.formatter = formatter
	}
}

func WithOutput(output io.Writer) LoggerOption {
	return func(l *logger) {
		l.output = output
	}
}

func WithHook(hook Hook) LoggerOption {
	return func(l *logger) {
		l.hooks = append(l.hooks, hook)
	}
}

func WithErrorHandler(handler ErrorHandler) LoggerOption {
	return func(l *logger) {
		l.errorHandler = handler
	}
}

// Log implements the core logging logic
func (l *logger) Log(entry *Entry) {
	// Add debugging log
	// fmt.Printf("Logging entry: %+v\n", entry)
	// fmt.Printf("Using formatter: %+v\n", l.formatter)
	// fmt.Printf("Writing to output: %+v\n", l.output)

	// Check log level first
	if entry.Level < l.level {
		return
	}

	// Thread safety
	l.mu.Lock()
	defer l.mu.Unlock()

	// Clone entry to prevent data races
	newEntry := entry.clone()

	// fmt.Printf("Real entry: %+v\n", entry)
	// fmt.Printf("Cloned entry: %+v\n", newEntry)

	// Add shared fields
	for k, v := range l.fields {
		if _, exists := newEntry.Fields[k]; !exists {
			newEntry.Fields[k] = v
		}
	}

	// Add component if set
	if l.component != "" {
		newEntry.Component = l.component
	}

	// Process hooks
	for _, hook := range l.hooks {
		if err := hook.Fire(newEntry); err != nil && l.errorHandler != nil {
			l.errorHandler(fmt.Errorf("hook execution failed: %w", err))
			continue // Continue with other hooks even if one fails
		}
	}

	// Format the entry
	output, err := l.formatter.Format(newEntry)
	if err != nil {
		if l.errorHandler != nil {
			l.errorHandler(fmt.Errorf("entry formatting failed: %w", err))
		}
		return
	}

	// fmt.Printf("Formatted output: %s\n", string(output))

	// Write to output
	_, err = l.output.Write(output)
	if err != nil {
		if l.errorHandler != nil {
			l.errorHandler(fmt.Errorf("log writing failed: %w", err))
		}
		return
	}

	// Handle fatal logs
	if entry.Level == FatalLevel {
		os.Exit(1)
	}
}

// Helper methods to create new loggers with added context
func (l *logger) WithField(key string, value interface{}) Logger {
	newLogger := l.clone()
	if newLogger.fields == nil {
		newLogger.fields = make(map[string]interface{})
	}
	newLogger.fields[key] = value
	return newLogger
}

func (l *logger) WithFields(fields map[string]interface{}) Logger {
	newLogger := l.clone()
	if newLogger.fields == nil {
		newLogger.fields = make(map[string]interface{})
	}
	for k, v := range fields {
		newLogger.fields[k] = v
	}
	return newLogger
}

func (l *logger) WithError(err error) Logger {
	newLogger := l.clone()
	if redisErr, ok := err.(*errors.RedisError); ok {
		newLogger.lastError = redisErr
	} else {
		newLogger.fields["error"] = err.Error()
	}
	return newLogger

}

// WithContext adds context information to the logger
func (l *logger) WithContext(ctx context.Context) Logger {
	if ctx == nil {
		return l
	}

	if traceID, ok := ctx.Value(TraceIDKey).(string); ok {
		return l.WithField("trace_id", traceID)
	}
	return l
}

func (l *logger) WithComponent(component string) Logger {
	newLogger := l.clone()
	newLogger.component = component
	return newLogger
}

// Logging methods for different levels
func (l *logger) log(level Level, msg string) {
	l.Log(NewEntry(l).
		WithField("level", level).
		WithField("message", msg))
}

func (l *logger) logf(level Level, format string, args ...interface{}) {
	l.log(level, fmt.Sprintf(format, args...))
}

func (l *logger) Trace(msg string) {
	if l.level <= TraceLevel {
		l.Log(&Entry{
			Level:     TraceLevel,
			Message:   msg,
			Timestamp: time.Now(),
			Fields:    l.fields,
		})
	}
}

func (l *logger) Debug(msg string) {
	if l.level <= DebugLevel {
		l.Log(&Entry{
			Level:     DebugLevel,
			Message:   msg,
			Timestamp: time.Now(),
			Fields:    l.fields,
		})
	}
}
func (l *logger) Info(msg string) {
	if l.level <= InfoLevel {
		l.Log(&Entry{
			Level:     InfoLevel,
			Message:   msg,
			Timestamp: time.Now(),
			Fields:    l.fields,
		})
	}
}

func (l *logger) Warn(msg string) {
	if l.level <= WarnLevel {
		l.Log(&Entry{
			Level:     WarnLevel,
			Message:   msg,
			Timestamp: time.Now(),
			Fields:    l.fields,
		})
	}
}

func (l *logger) Error(msg string) {
	if l.level <= ErrorLevel {
		l.Log(&Entry{
			Level:      ErrorLevel,
			Message:    msg,
			Timestamp:  time.Now(),
			Fields:     l.fields,
			RedisError: l.lastError,
		})
	}
}

func (l *logger) Fatal(msg string) {
	if l.level <= FatalLevel {
		l.Log(&Entry{
			Level:     FatalLevel,
			Message:   msg,
			Timestamp: time.Now(),
			Fields:    l.fields,
		})
	}
}

func (l *logger) Tracef(format string, args ...interface{}) { l.logf(TraceLevel, format, args...) }
func (l *logger) Debugf(format string, args ...interface{}) { l.logf(DebugLevel, format, args...) }
func (l *logger) Infof(format string, args ...interface{})  { l.logf(InfoLevel, format, args...) }
func (l *logger) Warnf(format string, args ...interface{})  { l.logf(WarnLevel, format, args...) }
func (l *logger) Errorf(format string, args ...interface{}) { l.logf(ErrorLevel, format, args...) }
func (l *logger) Fatalf(format string, args ...interface{}) { l.logf(FatalLevel, format, args...) }

// clone creates a copy of the logger
func (l *logger) clone() *logger {
	newLogger := &logger{
		level:        l.level,
		formatter:    l.formatter,
		output:       l.output,
		fields:       make(map[string]interface{}, len(l.fields)),
		component:    l.component,
		hooks:        make([]Hook, len(l.hooks)),
		errorHandler: l.errorHandler,
		lastError:    l.lastError,
	}

	// Copy fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}

	// Copy hooks
	copy(newLogger.hooks, l.hooks)

	return newLogger
}
