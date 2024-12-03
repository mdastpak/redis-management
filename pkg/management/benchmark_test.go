// pkg/management/benchmark_test.go
package management

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mdastpak/redis-management/config"
	"github.com/stretchr/testify/require"

	"github.com/alicebob/miniredis/v2"
)

// setupBenchmarkRedis creates a new miniredis instance and configures a new Redis service for benchmarking
// this fuction equlas to the setupTestRedis function in pkg/management/redis_test.go
// but it is modified to be used for benchmarking purposes
func setupBenchmarkRedis(b *testing.B) (*miniredis.Miniredis, *config.Config) {

	mr, err := miniredis.Run()
	require.NoError(b, err)

	cfg := &config.Config{
		Redis: config.RedisConfig{
			Host:                mr.Host(),
			Port:                mr.Port(),
			Password:            "",
			DB:                  "0",
			KeyPrefix:           "bench:",
			Timeout:             5,
			HashKeys:            false,
			HealthCheckInterval: 0,
			RetryAttempts:       1,
			RetryDelay:          10 * time.Millisecond,
			MaxRetryBackoff:     100 * time.Millisecond,
		},
		Pool: config.PoolConfig{
			Status:      true,
			Size:        10,
			MinIdle:     2,
			MaxIdleTime: 60,
			WaitTimeout: 5,
		},
		Bulk: config.BulkConfig{
			Status:          true,
			BatchSize:       100,
			FlushInterval:   1,
			MaxRetries:      1,
			ConcurrentFlush: true,
		},
	}

	return mr, cfg
}

func setupBenchmarkService(b *testing.B) (*miniredis.Miniredis, *RedisService) {
	mr, cfg := setupBenchmarkRedis(b)

	service, err := NewRedisService(cfg)
	if err != nil {
		mr.Close() // cleanup miniredis
		b.Fatalf("Failed to create Redis service: %v", err)
	}

	return mr, service
}

// Basic SET operation benchmark
func BenchmarkRedisService_Set(b *testing.B) {
	mr, service := setupBenchmarkService(b)
	defer mr.Close()
	defer service.Close()

	ctx := context.Background()
	value := "test-value"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := service.Set(ctx, key, value, time.Hour); err != nil {
			b.Fatalf("Failed to set key: %v", err)
		}
	}
}

// Basic GET operation benchmark
func BenchmarkRedisService_Get(b *testing.B) {
	mr, service := setupBenchmarkService(b)
	defer mr.Close()
	defer service.Close()

	ctx := context.Background()
	// Prepare data
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := service.Set(ctx, key, "test-value", time.Hour); err != nil {
			b.Fatalf("Failed to prepare data: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i)
		_, err := service.Get(ctx, key)
		if err != nil {
			b.Fatalf("Failed to get key: %v", err)
		}
	}
}

// Bulk operations benchmark
func BenchmarkRedisService_BulkOperations(b *testing.B) {
	mr, service := setupBenchmarkService(b)
	defer mr.Close()
	defer service.Close()

	ctx := context.Background()
	value := "bulk-test-value"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bulk:key:%d", i)
		if err := service.AddBulkOperation(ctx, "SET", key, value, time.Hour); err != nil {
			b.Fatalf("Failed to add bulk operation: %v", err)
		}
	}
}

// Concurrent operations benchmark
func BenchmarkRedisService_ConcurrentOperations(b *testing.B) {
	mr, service := setupBenchmarkService(b)
	defer mr.Close()
	defer service.Close()

	ctx := context.Background()
	value := "concurrent-test-value"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent:key:%d", i)
			if err := service.Set(ctx, key, value, time.Hour); err != nil {
				b.Fatalf("Failed concurrent set: %v", err)
			}
			i++
		}
	})
}

// Pool performance benchmark
func BenchmarkRedisService_PoolPerformance(b *testing.B) {
	mr, service := setupBenchmarkService(b)
	defer mr.Close()
	defer service.Close()

	ctx := context.Background()
	value := "pool-test-value"
	numGoroutines := 10

	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		for j := 0; j < numGoroutines; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("pool:key:%d", idx)
				if err := service.Set(ctx, key, value, time.Hour); err != nil {
					b.Errorf("Failed pool operation: %v", err)
				}
			}(i*numGoroutines + j)
		}
		wg.Wait()
	}
}

// Memory usage benchmark
func BenchmarkRedisService_MemoryUsage(b *testing.B) {
	mr, service := setupBenchmarkService(b)
	defer mr.Close()
	defer service.Close()

	ctx := context.Background()
	largeValue := string(make([]byte, 1024*1024)) // 1MB value

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("large:key:%d", i)
		if err := service.Set(ctx, key, largeValue, time.Hour); err != nil {
			b.Fatalf("Failed to set large value: %v", err)
		}
	}
}

func BenchmarkRedisService_ParallelPoolPerformance(b *testing.B) {
	mr, service := setupBenchmarkService(b)
	defer mr.Close()
	defer service.Close()

	ctx := context.Background()

	b.Run("Sequential Operations", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:seq:%d", i)
			err := service.Set(ctx, key, "value", time.Hour)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel Operations", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("bench:par:%d", i)
				err := service.Set(ctx, key, "value", time.Hour)
				if err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	})

	b.Run("Mixed Operations", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("bench:mixed:%d", i)

				// Set operation
				err := service.Set(ctx, key, "value", time.Hour)
				if err != nil {
					b.Fatal(err)
				}

				// Get operation
				_, err = service.Get(ctx, key)
				if err != nil {
					b.Fatal(err)
				}

				i++
			}
		})
	})
}
