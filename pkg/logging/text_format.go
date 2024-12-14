package logging

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// TextFormatter formats logs as human-readable text
type TextFormatter struct {
	// DisableTimestamp disables automatic timestamp field
	DisableTimestamp bool
	// TimestampFormat defines the format of timestamps
	TimestampFormat string
	// DisableColors disables colored output
	DisableColors bool
	// DisableCaller disables caller information
	DisableCaller bool
	// DisableSorting disables sorting of fields
	DisableSorting bool
	// FieldMap allows custom field names in the output
	FieldMap map[string]string
	// QuoteEmptyFields enables quoting of empty fields
	QuoteEmptyFields bool

	// Internal buffer pool
	bufferPool sync.Pool
}

// NewTextFormatter creates a new TextFormatter with default settings
func NewTextFormatter() *TextFormatter {
	return &TextFormatter{
		TimestampFormat: time.RFC3339,
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
func (f *TextFormatter) Format(entry *Entry) ([]byte, error) {
	b := f.getBuffer()
	defer f.putBuffer(b)

	// Write timestamp
	if !f.DisableTimestamp {
		timestampFormat := f.TimestampFormat
		if timestampFormat == "" {
			timestampFormat = time.RFC3339
		}
		f.appendKeyValue(b, f.getFieldName("time"), entry.Timestamp.Format(timestampFormat))
	}

	// Write level
	if !f.DisableColors {
		fmt.Fprintf(b, "\x1b[%dm", f.getColorByLevel(entry.Level))
	}
	f.appendKeyValue(b, f.getFieldName("level"), entry.Level.String())
	if !f.DisableColors {
		b.WriteString("\x1b[0m")
	}

	// Write caller
	if !f.DisableCaller && entry.CallerFunc != "" {
		f.appendKeyValue(b, f.getFieldName("caller"),
			fmt.Sprintf("%s:%d", entry.CallerFile, entry.CallerLine))
	}

	// Write trace ID
	if entry.TraceID != "" {
		f.appendKeyValue(b, f.getFieldName("trace_id"), entry.TraceID)
	}

	// Write operation
	if entry.Operation != "" {
		f.appendKeyValue(b, "operation", entry.Operation)
	}

	// Write component
	if entry.Component != "" {
		f.appendKeyValue(b, "component", entry.Component)
	}

	// Write duration
	if entry.Duration > 0 {
		f.appendKeyValue(b, "duration_ms",
			fmt.Sprintf("%.2f", float64(entry.Duration)/float64(time.Millisecond)))
	}

	// Write message
	f.appendKeyValue(b, f.getFieldName("msg"), entry.Message)

	// Write error
	if entry.RedisError != nil {
		f.appendKeyValue(b, f.getFieldName("error"), fmt.Sprintf("[%d] %s",
			entry.RedisError.Code, entry.RedisError.Error()))
		if len(entry.RedisError.Fields) > 0 {
			for k, v := range entry.RedisError.Fields {
				f.appendKeyValue(b, fmt.Sprintf("error_%s", k), fmt.Sprint(v))
			}
		}
	} else if entry.ErrorText != "" {
		f.appendKeyValue(b, f.getFieldName("error"), entry.ErrorText)
	}

	// Write custom fields
	f.writeFields(b, entry)

	b.WriteByte('\n')
	return b.Bytes(), nil
}

// getBuffer gets a buffer from the pool
func (f *TextFormatter) getBuffer() *bytes.Buffer {
	if f.bufferPool.New == nil {
		f.bufferPool.New = func() interface{} {
			return &bytes.Buffer{}
		}
	}
	return f.bufferPool.Get().(*bytes.Buffer)
}

// putBuffer returns a buffer to the pool
func (f *TextFormatter) putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	f.bufferPool.Put(buf)
}

func (f *TextFormatter) writeFields(b *bytes.Buffer, entry *Entry) {
	if len(entry.Fields) == 0 {
		return
	}

	var keys []string
	if !f.DisableSorting {
		keys = make([]string, 0, len(entry.Fields))
		for k := range entry.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	} else {
		keys = make([]string, 0, len(entry.Fields))
		for k := range entry.Fields {
			keys = append(keys, k)
		}
	}

	for _, key := range keys {
		f.appendKeyValue(b, key, entry.Fields[key])
	}
}

func (f *TextFormatter) appendKeyValue(b *bytes.Buffer, key string, value interface{}) {
	if b.Len() > 0 {
		b.WriteByte(' ')
	}

	b.WriteString(key)
	b.WriteByte('=')

	stringVal := fmt.Sprint(value)
	if f.needsQuoting(stringVal) {
		fmt.Fprintf(b, "%q", stringVal)
	} else {
		b.WriteString(stringVal)
	}
}

func (f *TextFormatter) needsQuoting(text string) bool {
	if f.QuoteEmptyFields && len(text) == 0 {
		return true
	}
	if strings.ContainsAny(text, " \t\r\n\"") {
		return true
	}
	return false
}

func (f *TextFormatter) getColorByLevel(level Level) int {
	switch level {
	case TraceLevel:
		return 37 // white
	case DebugLevel:
		return 36 // cyan
	case InfoLevel:
		return 32 // green
	case WarnLevel:
		return 33 // yellow
	case ErrorLevel:
		return 31 // red
	case FatalLevel:
		return 35 // magenta
	default:
		return 37 // white
	}
}

func (f *TextFormatter) getFieldName(defaultName string) string {
	if f.FieldMap == nil {
		return defaultName
	}
	if customName, ok := f.FieldMap[defaultName]; ok {
		return customName
	}
	return defaultName
}
