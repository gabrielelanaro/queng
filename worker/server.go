// Package worker runs physical plan tasks through a gRPC service.
package worker

import (
	"context"

	"github.com/gabrielelanaro/queng/batchserde"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/planserde"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server executes tasks using its local exchange store.
type Server struct {
	UnimplementedWorkerServer
	Exchange execution.ExchangeStore
}

func (s *Server) RunTask(ctx context.Context, request *TaskRequest) (*TaskResult, error) {
	if request.GetShufflePartitions() < 1 {
		return nil, status.Error(codes.InvalidArgument, "shuffle partition count must be positive")
	}
	plan, err := planserde.DeserializePlan(request.GetPlan())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "deserialize plan: %v", err)
	}
	executor := distributed.LocalExecutor{Exchange: s.Exchange, ShufflePartitions: int(request.GetShufflePartitions())}
	task := distributed.Task{JobID: request.GetJobId(), ID: int(request.GetTaskId()), PartitionID: int(request.GetPartitionId()), Stage: distributed.Stage{Plan: plan}}
	result := <-executor.Submit(task)
	if result.Err != nil {
		return nil, status.Errorf(codes.Internal, "execute task: %v", result.Err)
	}
	response := &TaskResult{}
	for _, batch := range result.Batches {
		defer batch.Release()
	}
	for _, batch := range result.Batches {
		data, err := batchserde.Encode(batch)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode result batch: %v", err)
		}
		response.Batches = append(response.Batches, data)
	}
	return response, nil
}
