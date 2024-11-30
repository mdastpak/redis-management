// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"redis-management/config"
	"redis-management/pkg/management"
)

func main() {
	// Load configuration
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize Redis management service
	redis, err := management.NewRedisService(cfg)
	if err != nil {
		log.Fatalf("Failed to create Redis service: %v", err)
	}
	defer redis.Close()

	// Example usage
	ctx := context.Background()
	err = redis.Set(ctx, "test_key", "test_value", time.Minute)
	if err != nil {
		log.Printf("Failed to set key: %v", err)
	}

	value, err := redis.Get(ctx, "test_key")
	if err != nil {
		log.Printf("Failed to get key: %v", err)
	}
	fmt.Printf("Retrieved value: %s\n", value)
}
