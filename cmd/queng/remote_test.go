package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type testTransport func(*http.Request) (*http.Response, error)

func (t testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return t(r) }

func TestRemoteQueryShowsProgressAndRows(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/queries":
			fmt.Fprint(w, `{"job_id":"abc"}`)
		case "/queries/abc/progress":
			fmt.Fprint(w, `{"status":"completed","stages":[{"id":0,"status":"completed","dependencies":[]}]}`)
		case "/queries/abc":
			fmt.Fprint(w, `{"status":"completed","result":{"columns":["price"],"rows":[[10]]}}`)
		default:
			http.NotFound(w, r)
		}
	})
	previous := http.DefaultClient.Transport
	http.DefaultClient.Transport = testTransport(func(req *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		return response.Result(), nil
	})
	t.Cleanup(func() { http.DefaultClient.Transport = previous })
	var output, progress bytes.Buffer
	if err := run([]string{"SELECT price FROM inventory", "--server", "http://coordinator"}, &output, &progress); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "price") || !strings.Contains(output.String(), "10") {
		t.Errorf("result = %q", output.String())
	}
	if !strings.Contains(progress.String(), "●") {
		t.Errorf("progress = %q", progress.String())
	}
}

func TestFailedStageDisplaysCross(t *testing.T) {
	stages := []progressStage{{ID: 0, Status: "completed"}, {ID: 1, Dependencies: []int{0}, Status: "failed"}}
	if got := formatStages(stages, "⠋"); got != "[ ● ]  [ ✕ ]" {
		t.Errorf("progress = %q", got)
	}
}
