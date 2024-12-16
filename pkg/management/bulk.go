package management

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mdastpak/redis-management/config"
	"github.com/mdastpak/redis-management/pkg/logging"
)

type BulkProcessor struct {
	service *RedisService
	queue   chan BulkOperation
	config  *config.BulkConfig
	status  atomic.Value // stores bool
	metrics *BulkMetrics
	logger  logging.Logger
}

type BulkOperation struct {
	Command   string
	Key       string
	Value     interface{}
	ExpiresAt time.Duration
	Result    chan error
	StartTime time.Time
}

type BulkResult struct {
	SuccessCount int
	FailedKeys   []string
	Errors       []error
	BatchSize    int
	ProcessTime  time.Duration
}

type BulkProcessorStatus struct {
	IsRunning      bool
	QueueSize      int
	LastBatchTime  time.Time
	ErrorRate      float64
	AverageLatency time.Duration
}

func NewBulkProcessor(service *RedisService, cfg *config.BulkConfig, logger logging.Logger) *BulkProcessor {
	processor := &BulkProcessor{
		service: service,
		queue:   make(chan BulkOperation, cfg.BatchSize),
		config:  cfg,
		logger:  logger.WithComponent("bulk"),
		metrics: NewBulkMetrics(),
	}

	processor.status.Store(false)

	return processor
}

func (bp *BulkProcessor) Start(ctx context.Context) {
	if !bp.status.CompareAndSwap(false, true) {
		bp.logger.Warn("Bulk processor is already running")
		return
	}

	bp.logger.Info("Starting bulk processor")
	go bp.processQueue(ctx)
}

func (bp *BulkProcessor) Stop() {
	// Prevent multiple stops
	if !bp.status.CompareAndSwap(true, false) {
		return
	}

	bp.logger.Info("Stopping bulk processor")

	// Signal processQueue to stop
	close(bp.queue)

	// Wait a short time for existing operations to complete
	time.Sleep(100 * time.Millisecond)
}

func (bp *BulkProcessor) AddOperation(ctx context.Context, command string, key string, value interface{}, expires time.Duration) error {
	if !bp.status.Load().(bool) {
		return fmt.Errorf("bulk processor is not running")
	}

	resultCh := make(chan error, 1)
	op := BulkOperation{
		Command:   command,
		Key:       key,
		Value:     value,
		ExpiresAt: expires,
		Result:    resultCh,
		StartTime: time.Now(),
	}

	select {
	case bp.queue <- op:
		select {
		case err := <-resultCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bp *BulkProcessor) processBatch(batch []BulkOperation) BulkResult {
	startTime := time.Now()
	result := BulkResult{
		BatchSize: len(batch),
	}

	// Check if service and client are available
	if bp.service == nil || bp.service.client == nil {
		err := fmt.Errorf("service or client is not available")
		for _, op := range batch {
			result.FailedKeys = append(result.FailedKeys, op.Key)
			result.Errors = append(result.Errors, err)
			op.Result <- err
		}
		return result
	}

	// Use a separate context for batch processing
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Group operations by database for efficiency
	opsByDB := make(map[int][]BulkOperation)
	for _, op := range batch {
		db, err := bp.service.keyMgr.GetShardIndex(op.Key)
		if err != nil {
			result.FailedKeys = append(result.FailedKeys, op.Key)
			result.Errors = append(result.Errors, err)
			op.Result <- err
			continue
		}
		opsByDB[db] = append(opsByDB[db], op)
	}

	pipe := bp.service.client.Pipeline()
	if pipe == nil {
		err := fmt.Errorf("failed to create pipeline")
		for _, op := range batch {
			result.FailedKeys = append(result.FailedKeys, op.Key)
			result.Errors = append(result.Errors, err)
			op.Result <- err
		}
		return result
	}
	defer pipe.Close()

	// Process each database group
	for db, ops := range opsByDB {
		// Select database once for group
		pipe.Do(ctx, "SELECT", db)

		// Add all operations to pipeline
		for _, op := range ops {
			finalKey := bp.service.keyMgr.GetKey(op.Key)
			pipe.Set(ctx, finalKey, op.Value, op.ExpiresAt)
		}

		// Execute pipeline with error handling
		_, err := pipe.Exec(ctx)
		if err != nil {
			// Handle batch error
			for _, op := range ops {
				result.FailedKeys = append(result.FailedKeys, op.Key)
				result.Errors = append(result.Errors, err)
				op.Result <- err
			}
		} else {
			// Mark operations as successful
			result.SuccessCount += len(ops)
			for _, op := range ops {
				op.Result <- nil
			}
		}
	}

	result.ProcessTime = time.Since(startTime)
	bp.updateMetrics(&result)

	return result
}

func (bp *BulkProcessor) processQueue(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(bp.config.FlushInterval) * time.Second)
	defer ticker.Stop()

	var batch []BulkOperation

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				bp.processBatch(batch)
			}
			return

		case op, ok := <-bp.queue:
			if !ok {
				if len(batch) > 0 {
					bp.processBatch(batch)
				}
				return
			}

			// Check if processor is still running
			if !bp.status.Load().(bool) {
				op.Result <- fmt.Errorf("bulk processor is stopped")
				continue
			}

			batch = append(batch, op)
			if len(batch) >= bp.config.BatchSize {
				if bp.config.ConcurrentFlush {
					batchCopy := make([]BulkOperation, len(batch))
					copy(batchCopy, batch)
					go bp.processBatch(batchCopy)
				} else {
					bp.processBatch(batch)
				}
				batch = make([]BulkOperation, 0, bp.config.BatchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 && bp.status.Load().(bool) {
				if bp.config.ConcurrentFlush {
					batchCopy := make([]BulkOperation, len(batch))
					copy(batchCopy, batch)
					go bp.processBatch(batchCopy)
				} else {
					bp.processBatch(batch)
				}
				batch = make([]BulkOperation, 0, bp.config.BatchSize)
			}
		}
	}
}

func (bp *BulkProcessor) updateMetrics(result *BulkResult) {
	bp.metrics.BatchesProcessed.Add(1)
	bp.metrics.OperationsProcessed.Add(int64(result.SuccessCount))
	bp.metrics.ErrorCount.Add(int64(len(result.FailedKeys)))

	// Update average latency
	currentAvg := bp.metrics.AverageLatency.Load().(time.Duration)
	processedBatches := bp.metrics.BatchesProcessed.Load()
	newAvg := (currentAvg*time.Duration(processedBatches-1) + result.ProcessTime) / time.Duration(processedBatches)
	bp.metrics.AverageLatency.Store(newAvg)

	bp.metrics.LastProcessedTime.Store(time.Now())
}

func (bp *BulkProcessor) GetStatus() BulkProcessorStatus {
	queueSize := len(bp.queue)
	totalOps := bp.metrics.OperationsProcessed.Load()
	errorCount := bp.metrics.ErrorCount.Load()
	errorRate := float64(0)
	if totalOps > 0 {
		errorRate = float64(errorCount) / float64(totalOps) * 100
	}

	return BulkProcessorStatus{
		IsRunning:      bp.status.Load().(bool),
		QueueSize:      queueSize,
		LastBatchTime:  bp.metrics.LastProcessedTime.Load().(time.Time),
		ErrorRate:      errorRate,
		AverageLatency: bp.metrics.AverageLatency.Load().(time.Duration),
	}
}

func (bp *BulkProcessor) GetMetrics() BulkMetrics {
	return *bp.metrics
}
