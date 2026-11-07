package executor

import (
	"context"
	"fmt"

	"github.com/gabrielelanaro/queng/batchserde"
	"github.com/gabrielelanaro/queng/planserde"
	"github.com/gabrielelanaro/queng/workerapi"
)

// RemoteExecutor submits tasks to a worker over gRPC.
type RemoteExecutor struct {
	Client            workerapi.WorkerClient
	ShufflePartitions int
}

func (e RemoteExecutor) Submit(ctx context.Context, task Task) <-chan TaskResult {
	result := make(chan TaskResult, 1)
	go func() {
		defer close(result)
		plan, err := planserde.SerializePlan(task.Stage.Plan)
		if err != nil {
			result <- TaskResult{Err: fmt.Errorf("serialize plan: %w", err)}
			return
		}
		response, err := e.Client.RunTask(ctx, &workerapi.TaskRequest{
			JobId: task.JobID, TaskId: int64(task.ID), PartitionId: int64(task.PartitionID),
			ShufflePartitions: int64(e.ShufflePartitions), Plan: plan,
		})
		if err != nil {
			result <- TaskResult{Err: fmt.Errorf("run remote task: %w", err)}
			return
		}
		var taskResult TaskResult
		for _, data := range response.Batches {
			batch, err := batchserde.Decode(data)
			if err != nil {
				for _, batch := range taskResult.Batches {
					batch.Release()
				}
				result <- TaskResult{Err: fmt.Errorf("decode result batch: %w", err)}
				return
			}
			taskResult.Batches = append(taskResult.Batches, batch)
		}
		result <- taskResult
	}()
	return result
}

var _ Executor = RemoteExecutor{}
