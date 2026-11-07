package coordinator

import (
	"sync"
	"time"

	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/executor"
)

// JobEvent records a stage transition or a task update.
type JobEvent struct {
	Kind      string
	StageID   int
	Time      time.Time
	TaskCount int
	Task      *executor.TaskEvent
}

// Job holds the stage graph and events for one query.
type Job struct {
	ID     string
	mu     sync.Mutex
	stages []distributed.Stage
	events []JobEvent
}

func NewJob(id string, stages []distributed.Stage) *Job {
	copied := make([]distributed.Stage, len(stages))
	copy(copied, stages)
	for i := range copied {
		copied[i].Dependencies = append([]int(nil), copied[i].Dependencies...)
	}
	return &Job{ID: id, stages: copied}
}

// Stages returns the job's stage graph.
func (j *Job) Stages() []distributed.Stage {
	j.mu.Lock()
	defer j.mu.Unlock()
	stages := make([]distributed.Stage, len(j.stages))
	copy(stages, j.stages)
	for i := range stages {
		stages[i].Dependencies = append([]int(nil), stages[i].Dependencies...)
	}
	return stages
}

// Events returns the events recorded so far.
func (j *Job) Events() []JobEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	events := append([]JobEvent(nil), j.events...)
	for i := range events {
		if events[i].Task != nil {
			task := *events[i].Task
			events[i].Task = &task
		}
	}
	return events
}

func (j *Job) record(event JobEvent) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, event)
}

func (j *Job) reportTask(event executor.TaskEvent) {
	j.record(JobEvent{Kind: "task", StageID: event.StageID, Time: event.Time, Task: &event})
}
