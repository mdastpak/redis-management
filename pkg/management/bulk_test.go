// pkg/management/bulk_test.go
package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBulkOperations(t *testing.T) {
	mr, cfg := setupTestRedis(t)
	defer mr.Close()

	service, err := NewRedisService(cfg)
	require.NoError(t, err)
	defer service.Close()

	ctx := context.Background()

	t.Run("Bulk Set Operations", func(t *testing.T) {
		// Send multiple operations
		for i := 0; i < 10; i++ {
			err := service.AddBulkOperation(ctx, "SET",
				fmt.Sprintf("key%d", i),
				fmt.Sprintf("value%d", i),
				time.Hour)
			assert.NoError(t, err)
		}

		// Wait for bulk processing
		time.Sleep(time.Duration(cfg.Bulk.FlushInterval+1) * time.Second)

		// Verify results
		for i := 0; i < 10; i++ {
			value, err := service.Get(ctx, fmt.Sprintf("key%d", i))
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("value%d", i), value)
		}
	})
}
