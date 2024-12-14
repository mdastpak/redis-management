package logging

import (
	"strings"
)

// Formatter interface defines methods for formatting log entries
type Formatter interface {
	Format(*Entry) ([]byte, error)
}

// Format represents the output format type
type Format string

const (
	// JSONFormat formats logs as JSON
	FormatJSON Format = "json"
	// TextFormat formats logs as human-readable text
	FormatText Format = "text"
)

// FormatFromString converts a string to Format type
func FormatFromString(format string) Format {
	switch strings.ToLower(format) {
	case "json":
		return FormatJSON
	case "text":
		return FormatText
	default:
		return FormatJSON
	}
}

// NewFormatter creates a new formatter based on the format type
func NewFormatter(format Format) Formatter {
	switch format {
	case FormatJSON:
		return NewJSONFormatter()
	case FormatText:
		return NewTextFormatter()
	default:
		return NewJSONFormatter()
	}
}
