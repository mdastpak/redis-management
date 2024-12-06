// pkg/management/bulk.go
package management

import (
	"context"
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
	// mu         sync.RWMutex
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
				bp.processBatch(batch)
			}
			return

		case op := <-bp.queue:
			batch = append(batch, op)
			if len(batch) >= bp.batchSize {
				if bp.concurrent {
					batchCopy := make([]BulkOperation, len(batch))
					copy(batchCopy, batch)
					go bp.processBatch(batchCopy)
				} else {
					bp.processBatch(batch)
				}
				batch = make([]BulkOperation, 0, bp.batchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				if bp.concurrent {
					batchCopy := make([]BulkOperation, len(batch))
					copy(batchCopy, batch)
					go bp.processBatch(batchCopy)
				} else {
					bp.processBatch(batch)
				}
				batch = make([]BulkOperation, 0, bp.batchSize)
			}
		}
	}
}

func (bp *BulkProcessor) processBatch(batch []BulkOperation) error {
	if len(batch) == 0 {
		return nil
	}

	pipe := bp.service.client.Pipeline()
	defer pipe.Close()

	// Group by database for efficiency
	opsByDB := make(map[int][]BulkOperation)
	for _, op := range batch {
		db, err := bp.service.keyMgr.GetShardIndex(op.Key)
		if err != nil {
			op.Result <- err
			continue
		}
		opsByDB[db] = append(opsByDB[db], op)
	}

	// Process each database group
	for db, ops := range opsByDB {
		// Select database once for group
		pipe.Do(context.Background(), "SELECT", db)

		// Add all operations to pipeline
		for _, op := range ops {
			finalKey := bp.service.keyMgr.GetKey(op.Key)
			// Set value with expiration in one command
			pipe.Set(context.Background(), finalKey, op.Value, op.ExpiresAt)
		}

		// Execute all operations in one go
		_, err := pipe.Exec(context.Background())

		// Handle results
		for _, op := range ops {
			op.Result <- err
		}
	}

	return nil
}
