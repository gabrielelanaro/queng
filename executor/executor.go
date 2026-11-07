// Package executor submits tasks for local or remote execution.
package executor

import (
	"context"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/execution"
	"time"
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

// TaskEvent describes a task lifecycle update from an executor.
type TaskEvent struct {
	JobID       string
	StageID     int
	TaskID      int
	PartitionID int
	Time        time.Time
	Kind        TaskEventKind
	Duration    time.Duration
}
type TaskEventKind int

const (
	TaskStarted TaskEventKind = iota
	TaskCompleted
)

// EventHandler may be called concurrently when tasks run in parallel.
type EventHandler func(TaskEvent)

// Executor submits tasks for execution.
type Executor interface {
	Submit(ctx context.Context, task Task, report EventHandler) <-chan TaskResult
}

// LocalExecutor executes tasks in the current process.
type LocalExecutor struct {
	Exchange          execution.ExchangeStore
	ShufflePartitions int
}

func (e LocalExecutor) Submit(ctx context.Context, task Task, report EventHandler) <-chan TaskResult {
	result := make(chan TaskResult, 1)
	go func() {
		defer close(result)
		started := time.Now()
		if report != nil {
			report(TaskEvent{JobID: task.JobID, StageID: task.Stage.ID, TaskID: task.ID, PartitionID: task.PartitionID, Time: started, Kind: TaskStarted})
		}
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
		if report != nil {
			finished := time.Now()
			report(TaskEvent{JobID: task.JobID, StageID: task.Stage.ID, TaskID: task.ID, PartitionID: task.PartitionID, Time: finished, Kind: TaskCompleted, Duration: finished.Sub(started)})
		}
		result <- TaskResult{Batches: batches}
	}()
	return result
}
