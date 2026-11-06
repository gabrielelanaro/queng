// Package execution defines services available while a physical plan runs.
package execution

import (
	"iter"

	"github.com/gabrielelanaro/queng/datasource"
)

// Context holds the job and executor services for one task execution.
type Context struct {
	JobID    string
	Exchange ExchangeStore
}

// ExchangeStore holds batches produced by a stage for subsequent stages.
type ExchangeStore interface {
	// Write borrows batch for the duration of the call. The store must retain
	// or copy any data it needs after Write returns.
	Write(jobID string, stageID int, batch datasource.RecordBatch) error

	// Read yields batches owned by the caller, who must release them.
	Read(jobID string, stageID int) iter.Seq2[datasource.RecordBatch, error]

	// Close releases resources held by the store.
	Close() error
}
