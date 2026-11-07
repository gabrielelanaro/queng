package coordinator_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/gabrielelanaro/queng/coordinator"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/executor"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/physicalplan"
	"github.com/gabrielelanaro/queng/remoteexchange"
	"github.com/gabrielelanaro/queng/worker"
	"github.com/gabrielelanaro/queng/workerapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func makeJoinStages(t *testing.T) []distributed.Stage {
	t.Helper()
	makeSource := func(name, contents string) *datasource.CSVSource {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		source, err := datasource.NewCSVSource(path, 10)
		if err != nil {
			t.Fatal(err)
		}
		return source
	}
	sales := makeSource("sales.csv", "product_id,sale\n1,10\n2,20\n")
	products := makeSource("products.csv", "product_id,product\n1,apple\n")
	schema := datasource.Schema{Fields: append(append([]datasource.Field{}, sales.Schema().Fields...), products.Schema().Fields...)}
	plan := physicalplan.HashJoinExec{
		Left: physicalplan.ScanExec{Source: sales}, Right: physicalplan.ScanExec{Source: products},
		Type: logicalplan.LeftJoin, LeftKeys: []int{0}, RightKeys: []int{0}, OutputSchema: schema,
	}
	stages, err := distributed.BuildStages(plan)
	if err != nil {
		t.Fatal(err)
	}
	return stages
}

func startWorkers(t *testing.T, tracker *coordinator.MemoryLocationTracker) map[string]executor.Executor {
	t.Helper()
	trackerClient := startCoordinator(t, tracker)
	listeners := map[string]*bufconn.Listener{"worker-a": bufconn.Listen(1024 * 1024), "worker-b": bufconn.Listen(1024 * 1024)}
	dial := grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		listener, ok := listeners[address]
		if !ok {
			return nil, fmt.Errorf("unknown worker %q", address)
		}
		return listener.Dial()
	})
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), dial}
	clients := worker.NewClients(options...)
	t.Cleanup(func() { clients.Close() })
	workers := make(map[string]executor.Executor)
	for address, listener := range listeners {
		store := remoteexchange.New(distributed.NewMemoryExchange(), trackerClient, clients)
		t.Cleanup(func() { store.Close() })
		server := grpc.NewServer()
		workerapi.RegisterWorkerServer(server, &worker.Server{Exchange: store})
		go server.Serve(listener)
		t.Cleanup(server.Stop)
		client, err := clients.Get(address)
		if err != nil {
			t.Fatal(err)
		}
		workers[address] = executor.RemoteExecutor{Client: client, ShufflePartitions: 2}
	}
	return workers
}

func TestSchedulerReturnsJoinedRows(t *testing.T) {
	stages := makeJoinStages(t)
	tracker := coordinator.NewMemoryLocationTracker()
	workers := startWorkers(t, tracker)
	scheduler, err := coordinator.NewScheduler(tracker, workers, 2)
	if err != nil {
		t.Fatal(err)
	}

	batches, err := scheduler.Run(context.Background(), "job-1", stages, func(executor.TaskEvent) {})
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, batch := range batches {
		for row := range batch.Column(0).Len() {
			rows = append(rows, fmt.Sprint(batch.Column(0).Value(row), ":", batch.Column(1).Value(row), ":", batch.Column(3).Value(row)))
		}
		batch.Release()
	}
	sort.Strings(rows)
	want := []string{"1:10:apple", "2:20:<nil>"}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("rows = %v; want %v", rows, want)
	}
}

func TestSchedulerReportsRemoteTaskEvents(t *testing.T) {
	stages := makeJoinStages(t)
	tracker := coordinator.NewMemoryLocationTracker()
	scheduler, err := coordinator.NewScheduler(tracker, startWorkers(t, tracker), 2)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var events []executor.TaskEvent
	batches, err := scheduler.Run(context.Background(), "events-job", stages, func(event executor.TaskEvent) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range batches {
		batch.Release()
	}
	counts := map[executor.TaskEventKind]int{}
	for _, event := range events {
		if event.JobID != "events-job" || event.Time.IsZero() {
			t.Errorf("invalid event: %+v", event)
		}
		counts[event.Kind]++
	}
	if counts[executor.TaskStarted] != 4 || counts[executor.TaskCompleted] != 4 {
		t.Errorf("events: %v; want 4 starts and 4 completions", counts)
	}
}
