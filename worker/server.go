// Package worker runs physical plan tasks through a gRPC service.
package worker

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gabrielelanaro/queng/batchserde"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/executor"
	"github.com/gabrielelanaro/queng/planserde"
	"github.com/gabrielelanaro/queng/workerapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server executes tasks using its local exchange store.
type Server struct {
	workerapi.UnimplementedWorkerServer
	Exchange execution.ExchangeStore
}

func (s *Server) RunTask(ctx context.Context, request *workerapi.TaskRequest) (*workerapi.TaskResult, error) {
	started := time.Now()
	log.Info().Str("job", request.GetJobId()).Int64("task", request.GetTaskId()).Int64("partition", request.GetPartitionId()).Msg("task started")
	if request.GetShufflePartitions() < 1 {
		return nil, status.Error(codes.InvalidArgument, "shuffle partition count must be positive")
	}
	plan, err := planserde.DeserializePlan(request.GetPlan())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "deserialize plan: %v", err)
	}
	localExecutor := executor.LocalExecutor{Exchange: s.Exchange, ShufflePartitions: int(request.GetShufflePartitions())}
	task := executor.Task{JobID: request.GetJobId(), ID: int(request.GetTaskId()), PartitionID: int(request.GetPartitionId()), Stage: distributed.Stage{Plan: plan}}
	result := <-localExecutor.Submit(ctx, task)
	if result.Err != nil {
		log.Error().Err(result.Err).Str("job", request.GetJobId()).Int64("task", request.GetTaskId()).Msg("task failed")
		return nil, status.Errorf(codes.Internal, "execute task: %v", result.Err)
	}
	response := &workerapi.TaskResult{}
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
	log.Info().Str("job", request.GetJobId()).Int64("task", request.GetTaskId()).Dur("duration", time.Since(started)).Msg("task completed")
	return response, nil
}

// ReadPartition streams one producer task's output for a shuffle partition.
func (s *Server) ReadPartition(request *workerapi.PartitionRequest, stream workerapi.Worker_ReadPartitionServer) error {
	if s.Exchange == nil {
		return status.Error(codes.FailedPrecondition, "exchange store is not configured")
	}
	key := execution.ExchangeKey{
		JobID: request.GetJobId(), StageID: int(request.GetStageId()),
		TaskID: int(request.GetTaskId()), PartitionID: int(request.GetPartitionId()),
	}
	log.Info().Str("job", key.JobID).Int("stage", key.StageID).Int("task", key.TaskID).Int("partition", key.PartitionID).Msg("serving shuffle partition")
	for batch, err := range s.Exchange.Read(key) {
		if err != nil {
			return status.Errorf(codes.Internal, "read exchange: %v", err)
		}
		data, err := batchserde.Encode(batch)
		batch.Release()
		if err != nil {
			return status.Errorf(codes.Internal, "encode exchange batch: %v", err)
		}
		if err := stream.Send(&workerapi.Batch{Data: data}); err != nil {
			return err
		}
	}
	return nil
}
