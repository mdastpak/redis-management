// Package errors provides error types and error handling utilities
package errors

import "fmt"

// ErrorCode represents a unique error code for Redis operations
type ErrorCode int

const (
	// System Level Errors (1000-1099)
	ErrSystemInternal ErrorCode = iota + 1000
	ErrSystemConfig
	ErrSystemResource
	ErrSystemShutdown
	ErrSystemMaintenance

	// Connection Level Errors (1100-1199)
	ErrConnectionFailed ErrorCode = iota + 1100
	ErrConnectionTimeout
	ErrConnectionClosed
	ErrConnectionPool
	ErrConnectionAuth

	// Operation Level Errors (1200-1299)
	ErrOperationInvalid ErrorCode = iota + 1200
	ErrOperationTimeout
	ErrOperationCanceled
	ErrOperationNotAllowed
	ErrOperationRateLimit

	// Data Level Errors (1300-1399)
	ErrDataNotFound ErrorCode = iota + 1300
	ErrDataInvalid
	ErrDataCorrupted
	ErrDataExists
	ErrDataType

	// Key Management Errors (1400-1499)
	ErrKeyInvalid ErrorCode = iota + 1400
	ErrKeyTooLong
	ErrKeyNotFound
	ErrKeyExists
	ErrKeyPattern

	// Bulk Operation Errors (1500-1599)
	ErrBulkFailed ErrorCode = iota + 1500
	ErrBulkPartial
	ErrBulkTimeout
	ErrBulkSize
	ErrBulkValidation
)

// String implements the Stringer interface for ErrorCode
func (c ErrorCode) String() string {
	switch c {
	// System errors
	case ErrSystemInternal:
		return "internal system error"
	case ErrSystemConfig:
		return "configuration error"
	case ErrSystemResource:
		return "system resource error"
	case ErrSystemShutdown:
		return "system shutdown in progress"
	case ErrSystemMaintenance:
		return "system in maintenance mode"

	// Connection errors
	case ErrConnectionFailed:
		return "connection failed"
	case ErrConnectionTimeout:
		return "connection timeout"
	case ErrConnectionClosed:
		return "connection closed"
	case ErrConnectionPool:
		return "connection pool error"
	case ErrConnectionAuth:
		return "authentication failed"

	// Operation errors
	case ErrOperationInvalid:
		return "invalid operation"
	case ErrOperationTimeout:
		return "operation timeout"
	case ErrOperationCanceled:
		return "operation canceled"
	case ErrOperationNotAllowed:
		return "operation not allowed"
	case ErrOperationRateLimit:
		return "rate limit exceeded"

	// Data errors
	case ErrDataNotFound:
		return "data not found"
	case ErrDataInvalid:
		return "invalid data"
	case ErrDataCorrupted:
		return "corrupted data"
	case ErrDataExists:
		return "data already exists"
	case ErrDataType:
		return "invalid data type"

	// Key errors
	case ErrKeyInvalid:
		return "invalid key"
	case ErrKeyTooLong:
		return "key too long"
	case ErrKeyNotFound:
		return "key not found"
	case ErrKeyExists:
		return "key already exists"
	case ErrKeyPattern:
		return "invalid key pattern"

	// Bulk operation errors
	case ErrBulkFailed:
		return "bulk operation failed"
	case ErrBulkPartial:
		return "partial bulk operation failure"
	case ErrBulkTimeout:
		return "bulk operation timeout"
	case ErrBulkSize:
		return "invalid bulk operation size"
	case ErrBulkValidation:
		return "bulk operation validation failed"

	default:
		return fmt.Sprintf("unknown error code: %d", c)
	}
}

// Category returns the error category based on the error code range
func (c ErrorCode) Category() string {
	switch {
	case c >= 1000 && c < 1100:
		return "System"
	case c >= 1100 && c < 1200:
		return "Connection"
	case c >= 1200 && c < 1300:
		return "Operation"
	case c >= 1300 && c < 1400:
		return "Data"
	case c >= 1400 && c < 1500:
		return "Key"
	case c >= 1500 && c < 1600:
		return "Bulk"
	default:
		return "Unknown"
	}
}

// IsRetryable returns true if the error code represents a retryable error
func (c ErrorCode) IsRetryable() bool {
	switch c {
	case ErrConnectionTimeout,
		ErrConnectionFailed,
		ErrOperationTimeout,
		ErrSystemResource,
		ErrBulkTimeout:
		return true
	default:
		return false
	}
}
