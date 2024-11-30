// examples/pool/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"redis-management/config"
	"redis-management/pkg/management"
)

func main() {
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Configure connection pool
	cfg.Pool.Status = true
	cfg.Pool.Size = 50
	cfg.Pool.MinIdle = 10
	cfg.Pool.MaxIdleTime = 300

	redis, err := management.NewRedisService(cfg)
	if err != nil {
		log.Fatalf("Failed to create Redis service: %v", err)
	}
	defer redis.Close()

	// Perform concurrent operations using pool
	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("pool:key:%d", i)
			value := fmt.Sprintf("value_%d", i)

			if err := redis.Set(ctx, key, value, time.Hour); err != nil {
				log.Printf("Failed to set key: %v", err)
				return
			}

			retrieved, err := redis.Get(ctx, key)
			if err != nil {
				log.Printf("Failed to get key: %v", err)
				return
			}

			if retrieved != value {
				log.Printf("Value mismatch for key %s: expected %s, got %s", key, value, retrieved)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("All concurrent operations completed")
}
