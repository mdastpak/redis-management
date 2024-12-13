package management

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkBulkOperations(b *testing.B) {
	scales := []int{1, 10, 100, 1000}

	for _, scale := range scales {
		b.Run(fmt.Sprintf("Scale_%dx", scale), func(b *testing.B) {
			mr, cfg := setupBenchmarkRedis(b)
			defer mr.Close()

			cfg.Bulk.BatchSize = 10 * scale

			service, err := NewRedisService(cfg)
			require.NoError(b, err)
			defer service.Close()

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ops := make([]BulkOperation, scale)
				for j := 0; j < scale; j++ {
					key := fmt.Sprintf("bench_test:key:%d:%d", i, j)
					value := fmt.Sprintf("value:%d:%d", i, j)
					ops[j] = BulkOperation{
						Command: "SET",
						Key:     key,
						Value:   value,
					}
				}
				b.StartTimer()

				for _, op := range ops {
					err := service.AddBulkOperation(ctx, op.Command, op.Key, op.Value, time.Hour)
					if err != nil {
						b.Fatalf("Failed to add operation: %v", err)
					}
				}
			}
		})
	}
}
