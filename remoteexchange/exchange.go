// Package remoteexchange fetches shuffle partitions from producer workers.
package remoteexchange

import (
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/gabrielelanaro/queng/batchserde"
	"github.com/gabrielelanaro/queng/coordinator"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/worker"
)

// Exchange stores local output and fetches upstream output from workers.
type Exchange struct {
	local       execution.ExchangeStore
	coordinator coordinator.CoordinatorClient
	workers     *worker.Clients
}

// New creates an exchange that owns local. The caller owns the coordinator connection and worker clients.
func New(local execution.ExchangeStore, tracker coordinator.CoordinatorClient, workers *worker.Clients) *Exchange {
	return &Exchange{local: local, coordinator: tracker, workers: workers}
}

func (e *Exchange) Write(key execution.ExchangeKey, batch datasource.RecordBatch) error {
	return e.local.Write(key, batch)
}

func (e *Exchange) Read(key execution.ExchangeKey) iter.Seq2[datasource.RecordBatch, error] {
	return e.local.Read(key)
}

// ReadPartition fetches a partition from every producer registered for the stage.
func (e *Exchange) ReadPartition(parent context.Context, jobID string, stageID, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		ctx, cancel := context.WithCancel(parent)
		defer cancel()
		result, err := e.coordinator.GetLocations(ctx, &coordinator.LocationRequest{JobId: jobID, StageId: int64(stageID)})
		if err != nil {
			yield(datasource.RecordBatch{}, fmt.Errorf("get producer locations: %w", err))
			return
		}
		for _, location := range result.Locations {
			if !e.readProducer(ctx, location, jobID, stageID, partitionID, yield) {
				return
			}
		}
	}
}

func (e *Exchange) readProducer(ctx context.Context, location *coordinator.ProducerLocation, jobID string, stageID, partitionID int, yield func(datasource.RecordBatch, error) bool) bool {
	client, err := e.workers.Get(location.WorkerAddress)
	if err != nil {
		yield(datasource.RecordBatch{}, fmt.Errorf("connect to worker %q: %w", location.WorkerAddress, err))
		return false
	}
	stream, err := client.ReadPartition(ctx, &worker.PartitionRequest{JobId: jobID, StageId: int64(stageID), TaskId: location.TaskId, PartitionId: int64(partitionID)})
	if err != nil {
		yield(datasource.RecordBatch{}, fmt.Errorf("read partition from worker %q: %w", location.WorkerAddress, err))
		return false
	}
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return true
		}
		if err != nil {
			yield(datasource.RecordBatch{}, fmt.Errorf("read partition from worker %q: %w", location.WorkerAddress, err))
			return false
		}
		batch, err := batchserde.Decode(message.Data)
		if err != nil {
			yield(datasource.RecordBatch{}, fmt.Errorf("decode exchange batch: %w", err))
			return false
		}
		if !yield(batch, nil) {
			return false
		}
	}
}

func (e *Exchange) Close() error { return e.local.Close() }
