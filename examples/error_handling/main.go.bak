// examples/error_handling/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mdastpak/redis-management/config"
	"github.com/mdastpak/redis-management/pkg/management"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Configure retry attempts
	cfg.Redis.RetryAttempts = 3
	cfg.Redis.RetryDelay = 100 * time.Millisecond
	cfg.Redis.MaxRetryBackoff = 1 * time.Second

	redis, err := management.NewRedisService(cfg)
	if err != nil {
		log.Fatalf("Failed to create Redis service: %v", err)
	}
	defer redis.Close()

	ctx := context.Background()

	// Example 1: Try to get non-existent key
	_, err = redis.Get(ctx, "non:existent:key")
	if err != nil {
		fmt.Printf("Example 1 - Expected error for non-existent key: %v\n", err)
	}

	// Example 2: Set and Get with invalid context
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Immediately cancel the context

	err = redis.Set(cancelCtx, "test:key", "value", time.Hour)
	if err != nil {
		fmt.Printf("Example 2 - Expected error for cancelled context on Set: %v\n", err)
	}

	// Example 3: Demonstrate retry mechanism
	badCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	err = redis.Set(badCtx, "timeout:key", "value", time.Hour)
	if err != nil {
		fmt.Printf("Example 3 - Expected error for timeout: %v\n", err)
	}
}
