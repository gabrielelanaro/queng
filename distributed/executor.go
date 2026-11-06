package distributed

import (
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
)

// Task identifies a stage to execute as part of a job.
type Task struct {
	JobID string
	Stage Stage
}

// TaskResult holds the result of a task. The receiver owns its batches and
// must release them.
type TaskResult struct {
	Batches []datasource.RecordBatch
	Err     error
}

// Executor submits tasks for execution.
type Executor interface {
	Submit(task Task) <-chan TaskResult
}

// LocalExecutor executes tasks in the current process.
type LocalExecutor struct {
	Exchange execution.ExchangeStore
}

func (e LocalExecutor) Submit(task Task) <-chan TaskResult {
	result := make(chan TaskResult, 1)
	go func() {
		defer close(result)
		var batches []datasource.RecordBatch
		for batch, err := range task.Stage.Plan.Execute(execution.Context{JobID: task.JobID, Exchange: e.Exchange}) {
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
