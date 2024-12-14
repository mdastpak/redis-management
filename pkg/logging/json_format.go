package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// JSONFormatter formats log entries as JSON
type JSONFormatter struct {
	// DisableTimestamp disables automatic timestamp field
	DisableTimestamp bool
	// TimestampFormat defines the format of timestamps
	TimestampFormat string
	// PrettyPrint enables indented JSON formatting
	PrettyPrint bool
	// FieldMap allows custom field names in the output
	FieldMap map[string]string
	// DisableHTMLEscape disables escaping of HTML characters
	DisableHTMLEscape bool
}

// NewJSONFormatter creates a new JSONFormatter with default settings
func NewJSONFormatter() *JSONFormatter {
	return &JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
		FieldMap: map[string]string{
			"time":     "timestamp",
			"msg":      "message",
			"level":    "level",
			"error":    "error",
			"caller":   "caller",
			"trace_id": "trace_id",
		},
	}
}

// Format implements the Formatter interface
func (f *JSONFormatter) Format(entry *Entry) ([]byte, error) {
	data := make(map[string]interface{})

	// Add basic fields
	if !f.DisableTimestamp {
		data[f.getFieldName("time")] = entry.Timestamp.Format(f.TimestampFormat)
	}

	data[f.getFieldName("msg")] = entry.Message
	data[f.getFieldName("level")] = entry.Level.String()

	// Add caller information if available
	if entry.CallerFunc != "" {
		data[f.getFieldName("caller")] = fmt.Sprintf("%s:%d", entry.CallerFile, entry.CallerLine)
		data["function"] = entry.CallerFunc
	}

	// Add trace information
	if entry.TraceID != "" {
		data[f.getFieldName("trace_id")] = entry.TraceID
	}

	// Add operation if available
	if entry.Operation != "" {
		data["operation"] = entry.Operation
	}

	// Add component if available
	if entry.Component != "" {
		data["component"] = entry.Component
	}

	// Add duration if available
	if entry.Duration > 0 {
		data["duration_ms"] = float64(entry.Duration) / float64(time.Millisecond)
	}

	// Add error information
	if entry.RedisError != nil {
		errorData := make(map[string]interface{})
		errorData["message"] = entry.RedisError.Message
		errorData["code"] = entry.RedisError.Code
		if len(entry.RedisError.Fields) > 0 {
			errorData["fields"] = entry.RedisError.Fields
		}
		data[f.getFieldName("error")] = errorData
	} else if entry.ErrorText != "" {
		data[f.getFieldName("error")] = entry.ErrorText
	}

	// Add custom fields
	for key, value := range entry.Fields {
		switch v := value.(type) {
		case error:
			// Convert error to string to prevent json.Marshal issues
			data[key] = v.Error()
		default:
			data[key] = v
		}
	}

	var encoder *json.Encoder
	buf := &bytes.Buffer{}
	encoder = json.NewEncoder(buf)
	encoder.SetEscapeHTML(!f.DisableHTMLEscape)

	if f.PrettyPrint {
		encoder.SetIndent("", "  ")
	}

	if err := encoder.Encode(data); err != nil {
		return nil, fmt.Errorf("failed to marshal log entry: %v", err)
	}

	return buf.Bytes(), nil
}

// getFieldName returns the custom field name if mapped, otherwise returns the default name
func (f *JSONFormatter) getFieldName(defaultName string) string {
	if f.FieldMap == nil {
		return defaultName
	}
	if customName, ok := f.FieldMap[defaultName]; ok {
		return customName
	}
	return defaultName
}
