// pkg/management/circuit_breaker_test.go

package management

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()

	t.Run("Normal Operation", func(t *testing.T) {
		cb := NewCircuitBreaker(3, time.Second, 1)
		assert.Equal(t, "CLOSED", cb.GetState(), "Should start in closed state")

		// Successful operations
		for i := 0; i < 5; i++ {
			err := cb.Execute(func() error {
				return nil
			})
			assert.NoError(t, err)
		}

		assert.Equal(t, "CLOSED", cb.GetState(), "Should remain closed after successful operations")
	})

	t.Run("Opening Circuit", func(t *testing.T) {
		cb := NewCircuitBreaker(3, time.Second, 1)
		testErr := errors.New("test error")

		// Generate failures
		for i := 0; i < 3; i++ {
			err := cb.Execute(func() error {
				return testErr
			})
			assert.Error(t, err)
		}

		assert.Equal(t, "OPEN", cb.GetState(), "Should open after threshold failures")

		// Verify circuit is open
		err := cb.Execute(func() error {
			return nil
		})
		assert.ErrorIs(t, err, ErrCircuitOpen)
	})

	t.Run("Half-Open State", func(t *testing.T) {
		cb := NewCircuitBreaker(3, 100*time.Millisecond, 1)
		testErr := errors.New("test error")

		// Open the circuit
		for i := 0; i < 3; i++ {
			_ = cb.Execute(func() error {
				return testErr
			})
		}

		// Wait for reset timeout
		time.Sleep(200 * time.Millisecond)

		// First request in half-open state
		err := cb.Execute(func() error {
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "CLOSED", cb.GetState(), "Should close after successful test request")
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		cb := NewCircuitBreaker(5, time.Second, 2)
		done := make(chan bool)

		// Start multiple goroutines
		for i := 0; i < 10; i++ {
			go func() {
				err := cb.Execute(func() error {
					time.Sleep(10 * time.Millisecond)
					return nil
				})
				assert.NoError(t, err)
				done <- true
			}()
		}

		// Wait for all operations
		for i := 0; i < 10; i++ {
			<-done
		}

		assert.Equal(t, "CLOSED", cb.GetState(), "Should handle concurrent operations")
	})
}
