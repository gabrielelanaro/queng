// Package executor submits tasks for local or remote execution.
package executor

import (
	"context"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/execution"
)

// Task identifies a stage to execute as part of a job.
type Task struct {
	JobID       string
	ID          int
	Stage       distributed.Stage
	PartitionID int
}

// TaskResult holds the result of a task. The receiver owns its batches and
// must release them.
type TaskResult struct {
	Batches []datasource.RecordBatch
	Err     error
}

// Executor submits tasks for execution.
type Executor interface {
	Submit(ctx context.Context, task Task) <-chan TaskResult
}

// LocalExecutor executes tasks in the current process.
type LocalExecutor struct {
	Exchange          execution.ExchangeStore
	ShufflePartitions int
}

func (e LocalExecutor) Submit(ctx context.Context, task Task) <-chan TaskResult {
	result := make(chan TaskResult, 1)
	go func() {
		defer close(result)
		var batches []datasource.RecordBatch
		execCtx := execution.Context{Context: ctx, JobID: task.JobID, TaskID: task.ID, ShufflePartitions: e.ShufflePartitions, Exchange: e.Exchange}
		for batch, err := range task.Stage.Plan.Execute(execCtx, task.PartitionID) {
			if err != nil {
				for _, batch := range batches {
					batch.Release()
				}
				result <- TaskResult{Err: err}
				return
			}
			batches = append(batches, batch)
		}
		result <- TaskResult{Batches: batches}
	}()
	return result
}
