package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
	"os"
)

type progressStage struct {
	ID             int    `json:"id"`
	Dependencies   []int  `json:"dependencies"`
	Status         string `json:"status"`
	TaskCount      int    `json:"task_count"`
	CompletedTasks int    `json:"completed_tasks"`
}
type queryProgress struct {
	Status string          `json:"status"`
	Error  string          `json:"error"`
	Stages []progressStage `json:"stages"`
}
type remoteResult struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Result struct {
		Columns []string `json:"columns"`
		Rows    [][]any  `json:"rows"`
	} `json:"result"`
}

func requestJSON(method, url string, body io.Reader, result any) error {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(response.Body)
		return fmt.Errorf("coordinator: %s", strings.TrimSpace(string(message)))
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	return decoder.Decode(result)
}

func runRemote(query, server string, stdout, stderr io.Writer) error {
	server = strings.TrimRight(server, "/")
	var submission struct {
		JobID string `json:"job_id"`
	}
	if err := requestJSON(http.MethodPost, server+"/queries", bytes.NewBufferString(query), &submission); err != nil {
		return err
	}
	start := time.Now()
	terminal := false
	if f, ok := stderr.(*os.File); ok {
		terminal = isatty.IsTerminal(f.Fd())
	}
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	for frame := 0; ; frame++ {
		var progress queryProgress
		if err := requestJSON(http.MethodGet, server+"/queries/"+submission.JobID+"/progress", nil, &progress); err != nil {
			return err
		}
		line := fmt.Sprintf("%s  %.1fs", formatStages(progress.Stages, frames[frame%len(frames)]), time.Since(start).Seconds())
		if terminal {
			fmt.Fprintf(stderr, "\r\033[2K%s", line)
		} else if frame == 0 || progress.Status != "running" {
			fmt.Fprintln(stderr, line)
		}
		if progress.Status != "running" {
			if terminal {
				fmt.Fprintln(stderr)
			}
			if progress.Status == "failed" {
				return fmt.Errorf("query failed: %s", progress.Error)
			}
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	var result remoteResult
	if err := requestJSON(http.MethodGet, server+"/queries/"+submission.JobID, nil, &result); err != nil {
		return err
	}
	rows := make([][]string, len(result.Result.Rows))
	for i, row := range result.Result.Rows {
		for _, value := range row {
			if value == nil {
				rows[i] = append(rows[i], "NULL")
			} else {
				rows[i] = append(rows[i], fmt.Sprint(value))
			}
		}
	}
	return writeTable(stdout, result.Result.Columns, rows)
}

func formatStages(stages []progressStage, spinner string) string {
	levels := map[int]int{}
	// Dependencies can be listed in any order.
	var level func(int, map[int]bool) int
	byID := map[int]progressStage{}
	for _, stage := range stages {
		byID[stage.ID] = stage
	}
	level = func(id int, visiting map[int]bool) int {
		if n, ok := levels[id]; ok {
			return n
		}
		if visiting[id] {
			return 0
		}
		visiting[id] = true
		n := 0
		for _, dep := range byID[id].Dependencies {
			if l := level(dep, visiting) + 1; l > n {
				n = l
			}
		}
		delete(visiting, id)
		levels[id] = n
		return n
	}
	groups := map[int][]string{}
	max := 0
	for _, stage := range stages {
		n := level(stage.ID, map[int]bool{})
		if n > max {
			max = n
		}
		symbol := "○"
		if stage.Status == "running" {
			symbol = spinner
		}
		if stage.Status == "completed" {
			symbol = "●"
		}
		if stage.Status == "failed" {
			symbol = "✕"
		}
		groups[n] = append(groups[n], symbol)
	}
	result := make([]string, 0, max+1)
	for i := 0; i <= max; i++ {
		result = append(result, "[ "+strings.Join(groups[i], " ")+" ]")
	}
	return strings.Join(result, "  ")
}
