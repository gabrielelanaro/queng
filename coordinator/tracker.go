// Package coordinator tracks where completed tasks wrote their output.
package coordinator

import "sync"

// OutputLocation identifies the worker holding one producer task's output.
type OutputLocation struct {
	TaskID        int
	WorkerAddress string
}

// LocationTracker records and retrieves producer locations for a stage.
type LocationTracker interface {
	Track(jobID string, stageID int, location OutputLocation)
	GetLocations(jobID string, stageID int) []OutputLocation
}

type stageKey struct {
	jobID   string
	stageID int
}

// MemoryLocationTracker keeps locations in memory for the coordinator.
type MemoryLocationTracker struct {
	mu     sync.RWMutex
	stages map[stageKey]map[int]OutputLocation
}

func NewMemoryLocationTracker() *MemoryLocationTracker {
	return &MemoryLocationTracker{stages: make(map[stageKey]map[int]OutputLocation)}
}

func (t *MemoryLocationTracker) Track(jobID string, stageID int, location OutputLocation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := stageKey{jobID: jobID, stageID: stageID}
	if t.stages[key] == nil {
		t.stages[key] = make(map[int]OutputLocation)
	}
	t.stages[key][location.TaskID] = location
}

func (t *MemoryLocationTracker) GetLocations(jobID string, stageID int) []OutputLocation {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var locations []OutputLocation
	for _, location := range t.stages[stageKey{jobID: jobID, stageID: stageID}] {
		locations = append(locations, location)
	}
	return locations
}
