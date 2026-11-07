package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/gabrielelanaro/queng/coordinator"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/parser"
	"github.com/gabrielelanaro/queng/physicalplan"
	"github.com/google/uuid"
)

type jobRunner interface {
	Run(context.Context, *coordinator.Job) ([]datasource.RecordBatch, error)
}

type jobEntry struct {
	job     *coordinator.Job
	mu      sync.RWMutex
	status  string
	result  json.RawMessage
	failure string
}

type jobRegistry struct {
	mu     sync.RWMutex
	jobs   map[string]*jobEntry
	runner jobRunner
	tables map[string]datasource.DataSource
}

func newJobRegistry(runner jobRunner, tables map[string]datasource.DataSource) *jobRegistry {
	return &jobRegistry{jobs: make(map[string]*jobEntry), runner: runner, tables: tables}
}

// submit starts a job independently of the HTTP request's lifetime.
func (r *jobRegistry) submit(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "POST a SQL query", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stmt, err := parser.Parse(string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logical, err := logicalplan.BuildLogicalPlan(stmt, r.tables)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	physical, err := physicalplan.BuildPhysicalPlan(logical)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stages, err := distributed.BuildStages(physical)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id := uuid.NewString()
	entry := &jobEntry{job: coordinator.NewJob(id, stages), status: "running"}
	r.mu.Lock()
	r.jobs[id] = entry
	r.mu.Unlock()
	go r.execute(entry, physical.Schema())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/queries/"+id)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": id})
}

func (r *jobRegistry) execute(entry *jobEntry, schema datasource.Schema) {
	batches, err := r.runner.Run(context.Background(), entry.job)
	if err != nil {
		entry.mu.Lock()
		entry.status = "failed"
		entry.failure = err.Error()
		entry.mu.Unlock()
		return
	}
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	result := queryResult{Columns: []string{}, Rows: [][]any{}}
	for _, field := range schema.Fields {
		result.Columns = append(result.Columns, field.Name)
	}
	for _, batch := range batches {
		if batch.ColumnCount() == 0 {
			continue
		}
		for row := range batch.Column(0).Len() {
			values := make([]any, batch.ColumnCount())
			for col := range batch.ColumnCount() {
				values[col] = batch.Column(col).Value(row)
			}
			result.Rows = append(result.Rows, values)
		}
	}
	data, err := json.Marshal(result)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err != nil {
		entry.status = "failed"
		entry.failure = fmt.Sprintf("encode result: %v", err)
		return
	}
	entry.result = data
	entry.status = "completed"
}

func (r *jobRegistry) get(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "GET a query", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(req.URL.Path, "/queries/")
	r.mu.RLock()
	entry := r.jobs[id]
	r.mu.RUnlock()
	if entry == nil {
		http.NotFound(w, req)
		return
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		JobID  string          `json:"job_id"`
		Status string          `json:"status"`
		Result json.RawMessage `json:"result,omitempty"`
		Error  string          `json:"error,omitempty"`
	}{id, entry.status, entry.result, entry.failure})
}
