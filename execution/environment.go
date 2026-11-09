// Package execution defines services available while a physical plan runs.
package execution

import (
	"context"
	"iter"

	"github.com/gabrielelanaro/queng/datasource"
)

// Environment holds the job information and services for one task execution.
type Environment struct {
	JobID             string
	TaskID            int
	ShufflePartitions int
	Exchange          ExchangeStore
}

// ExchangeKey identifies one producer's output for a shuffle partition.
type ExchangeKey struct {
	JobID       string
	StageID     int
	TaskID      int
	PartitionID int
}

// ExchangeStore holds batches produced by a stage for subsequent stages.
type ExchangeStore interface {
	// Write borrows batch for the duration of the call. The store must retain
	// or copy any data it needs after Write returns.
	Write(key ExchangeKey, batch datasource.RecordBatch) error

	// Read yields batches owned by the caller, who must release them.
	Read(key ExchangeKey) iter.Seq2[datasource.RecordBatch, error]

	// ReadPartition yields output from all producers for one stage partition.
	// The caller owns the yielded batches and must release them.
	ReadPartition(ctx context.Context, jobID string, stageID, partitionID int) iter.Seq2[datasource.RecordBatch, error]

	// Close releases resources held by the store.
	Close() error
}
