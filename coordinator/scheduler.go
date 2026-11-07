package coordinator

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/executor"
	"github.com/gabrielelanaro/queng/physicalplan"
)

// Scheduler runs stage tasks on workers and records their output locations.
type Scheduler struct {
	tracker           LocationTracker
	workers           map[string]executor.Executor
	shufflePartitions int
}

func NewScheduler(tracker LocationTracker, workers map[string]executor.Executor, shufflePartitions int) (*Scheduler, error) {
	if len(workers) == 0 {
		return nil, fmt.Errorf("no workers configured")
	}
	if shufflePartitions < 1 {
		return nil, fmt.Errorf("shuffle partition count must be positive")
	}
	return &Scheduler{tracker: tracker, workers: maps.Clone(workers), shufflePartitions: shufflePartitions}, nil
}

// Run executes a job and records stage and task events. The caller owns the returned batches.
func (s *Scheduler) Run(ctx context.Context, job *Job) ([]datasource.RecordBatch, error) {
	jobID := job.ID
	stages := job.Stages()
	if err := validateStages(stages); err != nil {
		return nil, err
	}
	addresses := make([]string, 0, len(s.workers))
	for address := range s.workers {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan stageResult, len(stages))
	started := make(map[int]bool)
	finished := make(map[int]bool)
	var output []datasource.RecordBatch
	var firstErr error
	running := 0
	for len(finished) < len(stages) {
		if firstErr == nil {
			for _, stage := range stages {
				if started[stage.ID] || !dependenciesFinished(stage, finished) {
					continue
				}
				started[stage.ID] = true
				running++
				go func(stage distributed.Stage) {
					batches, err := s.runStage(ctx, jobID, stage, addresses, job)
					_, writesExchange := stage.Plan.(distributed.ExchangeWriteExec)
					results <- stageResult{id: stage.ID, batches: batches, err: err, writesExchange: writesExchange}
				}(stage)
			}
		}
		if running == 0 {
			if firstErr != nil {
				for _, batch := range output {
					batch.Release()
				}
				return nil, firstErr
			}
			for _, batch := range output {
				batch.Release()
			}
			return nil, fmt.Errorf("stages contain cyclic dependencies")
		}
		result := <-results
		running--
		if result.err != nil {
			job.record(JobEvent{Kind: "stage_failed", StageID: result.id, Time: time.Now()})
			if firstErr == nil {
				firstErr = result.err
				cancel()
			}
		}
		finished[result.id] = true
		if result.writesExchange {
			for _, batch := range result.batches {
				batch.Release()
			}
		} else {
			output = result.batches
		}
	}
	if firstErr != nil {
		for _, batch := range output {
			batch.Release()
		}
		return nil, firstErr
	}
	return output, nil
}

type stageResult struct {
	id             int
	batches        []datasource.RecordBatch
	err            error
	writesExchange bool
}

func dependenciesFinished(stage distributed.Stage, finished map[int]bool) bool {
	for _, id := range stage.Dependencies {
		if !finished[id] {
			return false
		}
	}
	return true
}

type taskResult struct {
	taskID  int
	address string
	batches []datasource.RecordBatch
	err     error
}

func (s *Scheduler) runStage(ctx context.Context, jobID string, stage distributed.Stage, addresses []string, job *Job) ([]datasource.RecordBatch, error) {
	count, err := stagePartitionCount(stage.Plan, s.shufflePartitions)
	if err != nil {
		return nil, fmt.Errorf("stage %d: %w", stage.ID, err)
	}
	started := time.Now()
	job.record(JobEvent{Kind: "stage_started", StageID: stage.ID, Time: started, TaskCount: count})
	log.Info().Str("job", jobID).Int("stage", stage.ID).Int("tasks", count).Msg("stage started")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan taskResult, count)
	for partition := range count {
		address := addresses[partition%len(addresses)]
		go func(partition int, address string) {
			log.Info().Str("job", jobID).Int("stage", stage.ID).Int("task", partition).Str("worker", address).Msg("task submitted")
			task := executor.Task{JobID: jobID, ID: partition, PartitionID: partition, Stage: stage}
			response := <-s.workers[address].Submit(ctx, task, job.reportTask)
			results <- taskResult{taskID: partition, address: address, batches: response.Batches, err: response.Err}
		}(partition, address)
	}
	var batches []datasource.RecordBatch
	var completed []taskResult
	var firstErr error
	for range count {
		result := <-results
		batches = append(batches, result.batches...)
		if result.err != nil && firstErr == nil {
			firstErr = result.err
			cancel()
		}
		completed = append(completed, result)
	}
	if firstErr != nil {
		for _, batch := range batches {
			batch.Release()
		}
		log.Error().Err(firstErr).Str("job", jobID).Int("stage", stage.ID).Msg("stage failed")
		return nil, fmt.Errorf("stage %d: %w", stage.ID, firstErr)
	}
	if _, ok := stage.Plan.(distributed.ExchangeWriteExec); ok {
		for _, result := range completed {
			s.tracker.Track(jobID, stage.ID, OutputLocation{TaskID: result.taskID, WorkerAddress: result.address})
		}
	}
	job.record(JobEvent{Kind: "stage_completed", StageID: stage.ID, Time: time.Now(), TaskCount: count})
	log.Info().Str("job", jobID).Int("stage", stage.ID).Dur("duration", time.Since(started)).Msg("stage completed")
	return batches, nil
}

// stagePartitionCount finds the partitioned inputs of a stage. Inputs to a
// multi-input stage must have the same partition count.
func stagePartitionCount(plan physicalplan.PhysicalPlan, shufflePartitions int) (int, error) {
	count := -1
	var visit func(physicalplan.PhysicalPlan) error
	visit = func(node physicalplan.PhysicalPlan) error {
		var n int
		switch p := node.(type) {
		case physicalplan.ScanExec:
			n = p.Source.PartitionCount()
		case distributed.ExchangeReadExec:
			n = shufflePartitions
		default:
			for _, child := range node.Children() {
				if err := visit(child); err != nil {
					return err
				}
			}
			return nil
		}
		if n < 1 {
			return fmt.Errorf("input partition count must be positive")
		}
		if count != -1 && count != n {
			return fmt.Errorf("input partition counts differ")
		}
		count = n
		return nil
	}
	if err := visit(plan); err != nil {
		return 0, err
	}
	if count == -1 {
		return 0, fmt.Errorf("stage has no partitioned input")
	}
	return count, nil
}

// validateStages checks that dependencies exist and the job has one result stage.
func validateStages(stages []distributed.Stage) error {
	byID := make(map[int]distributed.Stage, len(stages))
	resultStages := 0
	for _, stage := range stages {
		if _, exists := byID[stage.ID]; exists {
			return fmt.Errorf("duplicate stage %d", stage.ID)
		}
		byID[stage.ID] = stage
		if _, writesExchange := stage.Plan.(distributed.ExchangeWriteExec); !writesExchange {
			resultStages++
		}
	}
	for _, stage := range stages {
		for _, dependency := range stage.Dependencies {
			producer, exists := byID[dependency]
			if !exists {
				return fmt.Errorf("unknown stage %d", dependency)
			}
			if _, writesExchange := producer.Plan.(distributed.ExchangeWriteExec); !writesExchange {
				return fmt.Errorf("stage %d does not write exchange output", dependency)
			}
		}
	}
	if resultStages != 1 {
		return fmt.Errorf("expected one result stage, got %d", resultStages)
	}
	return nil
}
