// examples/basic/main.go
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
	// Initialize config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create Redis service
	redis, err := management.NewRedisService(cfg)
	if err != nil {
		log.Fatalf("Failed to create Redis service: %v", err)
	}
	defer redis.Close()

	ctx := context.Background()

	// Basic Set/Get operations
	if err := redis.Set(ctx, "user:123", "John Doe", time.Hour); err != nil {
		log.Printf("Failed to set key: %v", err)
	}

	value, err := redis.Get(ctx, "user:123")
	if err != nil {
		log.Printf("Failed to get key: %v", err)
	}
	fmt.Printf("Retrieved value: %s\n", value)
}
