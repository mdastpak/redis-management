// examples/bulk_operations/main.go
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
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Enable bulk operations
	cfg.Bulk.Status = true
	cfg.Bulk.BatchSize = 1000
	cfg.Bulk.FlushInterval = 5

	redis, err := management.NewRedisService(cfg)
	if err != nil {
		log.Fatalf("Failed to create Redis service: %v", err)
	}
	defer redis.Close()

	ctx := context.Background()

	// Perform bulk operations
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("bulk:key:%d", i)
		value := fmt.Sprintf("value_%d", i)

		if err := redis.AddBulkOperation(ctx, "SET", key, value, time.Hour); err != nil {
			log.Printf("Failed to add bulk operation: %v", err)
		}
	}

	// Wait for operations to complete
	time.Sleep(time.Duration(cfg.Bulk.FlushInterval+1) * time.Second)
}
