// queng-coordinator accepts queries and schedules them on configured workers.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gabrielelanaro/queng/coordinator"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/executor"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/parser"
	"github.com/gabrielelanaro/queng/physicalplan"
	"github.com/gabrielelanaro/queng/worker"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type tablesFlag []string

func (t *tablesFlag) String() string         { return strings.Join(*t, ", ") }
func (t *tablesFlag) Set(value string) error { *t = append(*t, value); return nil }

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.000"})
	grpcAddress := flag.String("grpc-listen", ":9000", "tracker gRPC listen address")
	httpAddress := flag.String("http-listen", ":8080", "query HTTP listen address")
	workerAddresses := flag.String("workers", "localhost:9001,localhost:9002", "comma-separated worker addresses")
	partitions := flag.Int("shuffle-partitions", 2, "number of shuffle partitions")
	var tableArgs tablesFlag
	flag.Var(&tableArgs, "table", "CSV table as name=path (repeatable)")
	flag.Parse()
	tables := make(map[string]datasource.DataSource)
	for _, arg := range tableArgs {
		name, path, ok := strings.Cut(arg, "=")
		if !ok || name == "" || path == "" {
			log.Fatalf("invalid table %q", arg)
		}
		source, err := datasource.NewCSVSource(path, 1024)
		if err != nil {
			log.Fatalf("table %s: %v", name, err)
		}
		tables[name] = source
	}
	clients := worker.NewClients(grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer clients.Close()
	executors := make(map[string]executor.Executor)
	for _, address := range strings.Split(*workerAddresses, ",") {
		address = strings.TrimSpace(address)
		if address == "" {
			log.Fatal("empty worker address")
		}
		client, err := clients.Get(address)
		if err != nil {
			log.Fatal(err)
		}
		executors[address] = executor.RemoteExecutor{Client: client, ShufflePartitions: *partitions}
	}
	tracker := coordinator.NewMemoryLocationTracker()
	scheduler, err := coordinator.NewScheduler(tracker, executors, *partitions)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	coordinator.RegisterCoordinatorServer(server, coordinator.NewServer(tracker))
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("tracker: %v", err)
		}
	}()
	defer server.Stop()
	http.HandleFunc("/query", queryHandler(scheduler, tables))
	log.Printf("coordinator tracker on %s; query endpoint on %s", *grpcAddress, *httpAddress)
	log.Fatal(http.ListenAndServe(*httpAddress, nil))
}

type queryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

func queryHandler(scheduler *coordinator.Scheduler, tables map[string]datasource.DataSource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST a SQL query", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		statement, err := parser.Parse(string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		logical, err := logicalplan.BuildLogicalPlan(statement, tables)
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
		jobID := uuid.NewString()
		started := time.Now()
		zlog.Info().Str("job", jobID).Int("stages", len(stages)).Msg("query started")
		batches, err := scheduler.Run(r.Context(), jobID, stages, func(event executor.TaskEvent) {
			logger := zlog.Info().Str("job", event.JobID).Int("stage", event.StageID).Int("task", event.TaskID)
			switch event.Kind {
			case executor.TaskStarted:
				logger.Msg("task started")
			case executor.TaskCompleted:
				logger.Dur("duration", event.Duration).Msg("task completed")
			}
		})
		if err != nil {
			zlog.Error().Err(err).Str("job", jobID).Msg("query failed")
			http.Error(w, fmt.Sprintf("execute query: %v", err), http.StatusInternalServerError)
			return
		}
		result := queryResult{Columns: []string{}, Rows: [][]any{}}
		for _, field := range physical.Schema().Fields {
			result.Columns = append(result.Columns, field.Name)
		}
		for _, batch := range batches {
			if batch.ColumnCount() > 0 {
				for row := range batch.Column(0).Len() {
					values := make([]any, batch.ColumnCount())
					for col := range batch.ColumnCount() {
						values[col] = batch.Column(col).Value(row)
					}
					result.Rows = append(result.Rows, values)
				}
			}
			// Arrow string values may reference batch memory, so encode before release below.
		}
		zlog.Info().Str("job", jobID).Int("rows", len(result.Rows)).Dur("duration", time.Since(started)).Msg("query completed")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Printf("write query result: %v", err)
		}
		for _, batch := range batches {
			batch.Release()
		}
	}
}
