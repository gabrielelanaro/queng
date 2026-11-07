package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gabrielelanaro/queng/coordinator"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/executor"
)

func makeQueryRegistry(t *testing.T) *jobRegistry {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inventory.csv")
	if err := os.WriteFile(path, []byte("price\n10\n20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewCSVSource(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	workers := map[string]executor.Executor{"local": executor.LocalExecutor{ShufflePartitions: 1}}
	scheduler, err := coordinator.NewScheduler(coordinator.NewMemoryLocationTracker(), workers, 1)
	if err != nil {
		t.Fatal(err)
	}
	return newJobRegistry(scheduler, map[string]datasource.DataSource{"inventory": source})
}

func TestSubmittedQueryReturnsRows(t *testing.T) {
	registry := makeQueryRegistry(t)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/queries", strings.NewReader("SELECT price FROM inventory"))
	registry.submit(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("submit query: %d %s", response.Code, response.Body)
	}
	var submission struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &submission); err != nil || submission.JobID == "" {
		t.Fatalf("submit query response = %s; error = %v", response.Body, err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/queries/"+submission.JobID, nil)
		registry.get(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("get query: %d %s", response.Code, response.Body)
		}
		var job struct {
			Status string      `json:"status"`
			Result queryResult `json:"result"`
			Error  string      `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status == "failed" {
			t.Fatalf("query failed: %s", job.Error)
		}
		if job.Status == "completed" {
			want := queryResult{Columns: []string{"price"}, Rows: [][]any{{float64(10)}, {float64(20)}}}
			if !reflect.DeepEqual(job.Result, want) {
				t.Errorf("query result = %#v; want %#v", job.Result, want)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("query did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}
