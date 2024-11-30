// pkg/management/bulk.go
package management

import (
	"context"
	"sync"
	"time"
)

// BulkOperation represents a single operation in the bulk queue
type BulkOperation struct {
	Command   string
	Key       string
	Value     interface{}
	ExpiresAt time.Duration
	Result    chan error
}

// BulkProcessor handles bulk operations
type BulkProcessor struct {
	service    *RedisService
	queue      chan BulkOperation
	batchSize  int
	interval   time.Duration
	maxRetries int
	concurrent bool
	mu         sync.RWMutex
}

// NewBulkProcessor creates a new bulk processor
func NewBulkProcessor(service *RedisService) *BulkProcessor {
	return &BulkProcessor{
		service:    service,
		queue:      service.bulkQueue,
		batchSize:  service.cfg.Bulk.BatchSize,
		interval:   time.Duration(service.cfg.Bulk.FlushInterval) * time.Second,
		maxRetries: service.cfg.Bulk.MaxRetries,
		concurrent: service.cfg.Bulk.ConcurrentFlush,
	}
}

// Start begins processing bulk operations
func (bp *BulkProcessor) Start(ctx context.Context) {
	go bp.processQueue(ctx)
}

// processQueue handles the bulk operation queue
func (bp *BulkProcessor) processQueue(ctx context.Context) {
	ticker := time.NewTicker(bp.interval)
	defer ticker.Stop()

	var batch []BulkOperation

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				bp.flush(batch)
			}
			return

		case op := <-bp.queue:
			batch = append(batch, op)
			if len(batch) >= bp.batchSize {
				if bp.concurrent {
					go bp.flush(batch)
				} else {
					bp.flush(batch)
				}
				batch = make([]BulkOperation, 0, bp.batchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				if bp.concurrent {
					go bp.flush(batch)
				} else {
					bp.flush(batch)
				}
				batch = make([]BulkOperation, 0, bp.batchSize)
			}
		}
	}
}

// flush processes a batch of operations
func (bp *BulkProcessor) flush(batch []BulkOperation) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	pipe := bp.service.client.Pipeline()

	// Group operations by database
	opsByDB := make(map[int][]BulkOperation)
	for _, op := range batch {
		db, err := bp.service.keyMgr.GetShardIndex(op.Key)
		if err != nil {
			op.Result <- err
			continue
		}
		opsByDB[db] = append(opsByDB[db], op)
	}

	// Process operations for each database
	for db, ops := range opsByDB {
		// Select database
		pipe.Do(context.Background(), "SELECT", db)

		// Add operations to pipeline
		for _, op := range ops {
			key := bp.service.keyMgr.GetKey(op.Key)
			switch op.Command {
			case "SET":
				pipe.Set(context.Background(), key, op.Value, op.ExpiresAt)
			case "DEL":
				pipe.Del(context.Background(), key)
				// Add other commands as needed
			}
		}
	}

	// Execute pipeline
	_, err := pipe.Exec(context.Background())

	// Handle results
	for _, op := range batch {
		op.Result <- err
	}
}
