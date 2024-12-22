package management

import (
	"log"
	"os"

	"redis-management/config"
)

// RedisLogger implements the Logger interface for Redis service
type RedisLogger struct {
	logger *log.Logger
	level  string
}

// initializeLogger creates a new logger based on configuration
func initializeLogger(cfg *config.Config) Logger {
	var logWriter = os.Stdout
	if cfg.Logging.Output == "file" && cfg.Logging.FilePath != "" {
		file, err := os.OpenFile(cfg.Logging.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			logWriter = file
		}
	}

	return &RedisLogger{
		logger: log.New(logWriter, "[REDIS] ", log.LstdFlags|log.Lmicroseconds),
		level:  cfg.Logging.Level,
	}
}

// Info logs informational messages
func (l *RedisLogger) Info(msg string) {
	if l.shouldLog("info") {
		l.logger.Printf("[INFO] %s", msg)
	}
}

// Error logs error messages
func (l *RedisLogger) Error(msg string, err error) {
	if l.shouldLog("error") {
		l.logger.Printf("[ERROR] %s: %v", msg, err)
	}
}

// Warn logs warning messages
func (l *RedisLogger) Warn(msg string) {
	if l.shouldLog("warn") {
		l.logger.Printf("[WARN] %s", msg)
	}
}

// shouldLog checks if the message should be logged based on configured level
func (l *RedisLogger) shouldLog(msgLevel string) bool {
	levels := map[string]int{
		"debug": 0,
		"info":  1,
		"warn":  2,
		"error": 3,
	}

	configuredLevel, exists := levels[l.level]
	if !exists {
		return true // If level is not configured, log everything
	}

	msgLevelValue, exists := levels[msgLevel]
	if !exists {
		return true // If message level is unknown, log it
	}

	return msgLevelValue >= configuredLevel
}

// Close releases any resources used by the logger
func (l *RedisLogger) Close() error {
	// If we're logging to a file, close it
	if writer, ok := l.logger.Writer().(*os.File); ok && writer != os.Stdout && writer != os.Stderr {
		return writer.Close()
	}
	return nil
}
