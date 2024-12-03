// pkg/management/circuit_breaker.go

package management

import (
	"errors"
	"sync/atomic"
	"time"
)

// Circuit breaker states
const (
	StateClosed   = iota // Normal operation
	StateOpen            // Not allowing operations
	StateHalfOpen        // Testing if service is healthy
)

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

type CircuitBreaker struct {
	failures     int64         // Current number of consecutive failures
	threshold    int64         // Number of failures before opening
	resetAfter   time.Duration // Time to wait before attempting reset
	lastFailure  time.Time     // Time of last failure
	state        int32         // Current state of the circuit breaker
	maxHalfOpen  int32         // Maximum concurrent requests in half-open state
	halfOpenReqs int32         // Current number of requests in half-open state
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(threshold int64, resetAfter time.Duration, maxHalfOpen int32) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:   threshold,
		resetAfter:  resetAfter,
		state:       StateClosed,
		maxHalfOpen: maxHalfOpen,
	}
}

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.AllowRequest() {
		return ErrCircuitOpen
	}

	err := fn()
	cb.RecordResult(err)
	return err
}

// AllowRequest checks if a request should be allowed
func (cb *CircuitBreaker) AllowRequest() bool {
	state := atomic.LoadInt32(&cb.state)

	switch state {
	case StateClosed:
		return true

	case StateOpen:
		if time.Since(cb.lastFailure) > cb.resetAfter {
			// Try moving to half-open state
			if atomic.CompareAndSwapInt32(&cb.state, StateOpen, StateHalfOpen) {
				atomic.StoreInt32(&cb.halfOpenReqs, 0)
			}
			return true
		}
		return false

	case StateHalfOpen:
		// Allow limited requests in half-open state
		reqs := atomic.AddInt32(&cb.halfOpenReqs, 1)
		if reqs > cb.maxHalfOpen {
			atomic.AddInt32(&cb.halfOpenReqs, -1)
			return false
		}
		return true

	default:
		return false
	}
}

// RecordResult records the result of a request
func (cb *CircuitBreaker) RecordResult(err error) {
	if err != nil {
		cb.recordFailure()
	} else {
		cb.recordSuccess()
	}
}

// recordFailure handles a failed request
func (cb *CircuitBreaker) recordFailure() {
	failures := atomic.AddInt64(&cb.failures, 1)
	cb.lastFailure = time.Now()

	if failures >= cb.threshold {
		atomic.StoreInt32(&cb.state, StateOpen)
	}
}

// recordSuccess handles a successful request
func (cb *CircuitBreaker) recordSuccess() {
	atomic.StoreInt64(&cb.failures, 0)
	if atomic.LoadInt32(&cb.state) == StateHalfOpen {
		atomic.StoreInt32(&cb.state, StateClosed)
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() string {
	switch atomic.LoadInt32(&cb.state) {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF-OPEN"
	default:
		return "UNKNOWN"
	}
}
