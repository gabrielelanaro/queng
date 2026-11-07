package executor

import (
	"context"
	"fmt"
	"io"

	"github.com/gabrielelanaro/queng/batchserde"
	"github.com/gabrielelanaro/queng/planserde"
	"github.com/gabrielelanaro/queng/workerapi"
)

// RemoteExecutor submits tasks to a worker over gRPC.
type RemoteExecutor struct {
	Client            workerapi.WorkerClient
	ShufflePartitions int
}

func (e RemoteExecutor) Submit(ctx context.Context, task Task, report EventHandler) <-chan TaskResult {
	result := make(chan TaskResult, 1)
	go func() {
		defer close(result)
		plan, err := planserde.SerializePlan(task.Stage.Plan)
		if err != nil {
			result <- TaskResult{Err: fmt.Errorf("serialize plan: %w", err)}
			return
		}
		stream, err := e.Client.RunTask(ctx, &workerapi.TaskRequest{
			JobId: task.JobID, TaskId: int64(task.ID), StageId: int64(task.Stage.ID), PartitionId: int64(task.PartitionID),
			ShufflePartitions: int64(e.ShufflePartitions), Plan: plan,
		})
		if err != nil {
			result <- TaskResult{Err: fmt.Errorf("run remote task: %w", err)}
			return
		}
		var response *workerapi.TaskResult
		for {
			message, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				result <- TaskResult{Err: fmt.Errorf("run remote task: %w", err)}
				return
			}
			if event := message.GetEvent(); event != nil && report != nil {
				update := TaskEvent{JobID: task.JobID, StageID: task.Stage.ID, TaskID: task.ID, PartitionID: task.PartitionID, Time: event.GetTime().AsTime()}
				switch {
				case event.GetStarted() != nil:
					update.Kind = TaskStarted
				case event.GetCompleted() != nil:
					update.Kind = TaskCompleted
					update.Duration = event.GetCompleted().GetDuration().AsDuration()
				default:
					continue
				}
				report(update)
			}
			if message.GetResult() != nil {
				response = message.GetResult()
			}
		}
		if response == nil {
			result <- TaskResult{Err: fmt.Errorf("remote task returned no result")}
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
