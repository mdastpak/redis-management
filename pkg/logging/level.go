package logging

import (
	"fmt"
	"strings"
)

// Level represents the severity level of a log message
type Level int32

const (
	// TraceLevel defines trace-level messages (most verbose)
	TraceLevel Level = iota
	// DebugLevel defines debug-level messages
	DebugLevel
	// InfoLevel defines informational messages
	InfoLevel
	// WarnLevel defines warning messages
	WarnLevel
	// ErrorLevel defines error messages
	ErrorLevel
	// FatalLevel defines fatal messages (least verbose)
	// Application will exit after logging fatal message
	FatalLevel
)

// AllLevels contains all logging levels in ascending order of severity
var AllLevels = []Level{
	TraceLevel,
	DebugLevel,
	InfoLevel,
	WarnLevel,
	ErrorLevel,
	FatalLevel,
}

// String returns the string representation of the log level
func (l Level) String() string {
	switch l {
	case TraceLevel:
		return "TRACE"
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return fmt.Sprintf("UNKNOWN[%d]", l)
	}
}

// ParseLevel converts a level string to a Level value
func ParseLevel(level string) (Level, error) {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "TRACE":
		return TraceLevel, nil
	case "DEBUG":
		return DebugLevel, nil
	case "INFO":
		return InfoLevel, nil
	case "WARN", "WARNING":
		return WarnLevel, nil
	case "ERROR":
		return ErrorLevel, nil
	case "FATAL":
		return FatalLevel, nil
	}
	return InfoLevel, fmt.Errorf("invalid log level: %q", level)
}

// MarshalText implements encoding.TextMarshaler interface
func (l Level) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler interface
func (l *Level) UnmarshalText(text []byte) error {
	level, err := ParseLevel(string(text))
	if err != nil {
		return err
	}
	*l = level
	return nil
}

// ColorString returns the colored string representation of the log level
// This is useful for console output
func (l Level) ColorString() string {
	var color string
	switch l {
	case TraceLevel:
		color = "\033[37m" // White
	case DebugLevel:
		color = "\033[36m" // Cyan
	case InfoLevel:
		color = "\033[32m" // Green
	case WarnLevel:
		color = "\033[33m" // Yellow
	case ErrorLevel:
		color = "\033[31m" // Red
	case FatalLevel:
		color = "\033[35m" // Magenta
	default:
		color = "\033[0m" // Reset
	}
	return fmt.Sprintf("%s%s\033[0m", color, l.String())
}

// IsValid checks if the level is valid
func (l Level) IsValid() bool {
	switch l {
	case TraceLevel, DebugLevel, InfoLevel, WarnLevel, ErrorLevel, FatalLevel:
		return true
	default:
		return false
	}
}

// Enable returns true if the level should be logged when the logging level is set to l
func (l Level) Enable(other Level) bool {
	return other >= l
}

// LevelFromConfig converts a config level string to a Level, defaulting to InfoLevel
func LevelFromConfig(configLevel string) Level {
	if level, err := ParseLevel(configLevel); err == nil {
		return level
	}
	return InfoLevel
}

// IsTerminalLevel returns true if the level should terminate the application
func (l Level) IsTerminalLevel() bool {
	return l == FatalLevel
}
