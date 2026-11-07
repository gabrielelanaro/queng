package remoteexchange_test

import (
	"context"
	"io"
	"net"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gabrielelanaro/queng/coordinator"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/planserde"
	"github.com/gabrielelanaro/queng/remoteexchange"
	"github.com/gabrielelanaro/queng/worker"
	"github.com/gabrielelanaro/queng/workerapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func startServer(t *testing.T, register func(*grpc.Server)) *bufconn.Listener {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener
}

type blockingCoordinator struct {
	coordinator.UnimplementedCoordinatorServer
	started  chan struct{}
	canceled chan struct{}
}

func (b *blockingCoordinator) GetLocations(ctx context.Context, _ *coordinator.LocationRequest) (*coordinator.LocationResponse, error) {
	close(b.started)
	<-ctx.Done()
	close(b.canceled)
	return nil, ctx.Err()
}

func TestCancelTaskCancelsExchangeLookup(t *testing.T) {
	tracker := &blockingCoordinator{started: make(chan struct{}), canceled: make(chan struct{})}
	trackerListener := startServer(t, func(server *grpc.Server) { coordinator.RegisterCoordinatorServer(server, tracker) })
	conn, err := grpc.NewClient("passthrough:///coordinator", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return trackerListener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	clients := worker.NewClients(grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(func() { clients.Close() })
	exchange := remoteexchange.New(distributed.NewMemoryExchange(), coordinator.NewCoordinatorClient(conn), clients)
	t.Cleanup(func() { exchange.Close() })
	workerListener := startServer(t, func(server *grpc.Server) { workerapi.RegisterWorkerServer(server, &worker.Server{Exchange: exchange}) })
	workerConn, err := grpc.NewClient("passthrough:///worker", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return workerListener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { workerConn.Close() })
	plan, err := planserde.SerializePlan(distributed.ExchangeReadExec{StageID: 3, Partitioning: distributed.HashPartitioning{Keys: []int{0}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		stream, err := workerapi.NewWorkerClient(workerConn).RunTask(ctx, &workerapi.TaskRequest{JobId: "job", ShufflePartitions: 1, Plan: plan})
		if err == nil {
			for err == nil {
				_, err = stream.Recv()
			}
		}
		done <- err
	}()
	select {
	case <-tracker.started:
	case <-time.After(5 * time.Second):
		t.Fatal("exchange lookup did not start")
	}
	cancel()
	select {
	case <-tracker.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("exchange lookup was not cancelled")
	}
	if err := <-done; err == nil {
		t.Fatal("cancelled task succeeded")
	}
}

func writeBatch(t *testing.T, store execution.ExchangeStore, taskID int, values ...any) {
	t.Helper()
	col, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, values)
	if err != nil {
		t.Fatal(err)
	}
	batch := datasource.NewRecordBatch(datasource.Schema{Fields: []datasource.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}}, []datasource.ColumnVector{col})
	defer batch.Release()
	if err := store.Write(execution.ExchangeKey{JobID: "job", StageID: 3, TaskID: taskID, PartitionID: 1}, batch); err != nil {
		t.Fatal(err)
	}
}

func TestReadPartitionFetchesFromAllProducerWorkers(t *testing.T) {
	tracker := coordinator.NewMemoryLocationTracker()
	listeners := make(map[string]*bufconn.Listener)
	for i, address := range []string{"worker-a", "worker-b"} {
		local := distributed.NewMemoryExchange()
		t.Cleanup(func() { local.Close() })
		taskID := i + 7
		writeBatch(t, local, taskID, int64(taskID))
		tracker.Track("job", 3, coordinator.OutputLocation{TaskID: taskID, WorkerAddress: address})
		listeners[address] = startServer(t, func(server *grpc.Server) { workerapi.RegisterWorkerServer(server, &worker.Server{Exchange: local}) })
	}
	coordinatorListener := startServer(t, func(server *grpc.Server) {
		coordinator.RegisterCoordinatorServer(server, coordinator.NewServer(tracker))
	})
	conn, err := grpc.NewClient("passthrough:///coordinator", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return coordinatorListener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	local := distributed.NewMemoryExchange()
	var connections atomic.Int32
	clients := worker.NewClients(grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		connections.Add(1)
		listener := listeners[address]
		if listener == nil {
			return nil, io.EOF
		}
		return listener.Dial()
	}))
	t.Cleanup(func() { clients.Close() })
	exchange := remoteexchange.New(local, coordinator.NewCoordinatorClient(conn), clients)
	t.Cleanup(func() { exchange.Close() })

	for range 2 {
		var ids []int
		for batch, err := range exchange.ReadPartition(context.Background(), "job", 3, 1) {
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, int(batch.Column(0).Value(0).(int64)))
			batch.Release()
		}
		sort.Ints(ids)
		if !reflect.DeepEqual(ids, []int{7, 8}) {
			t.Errorf("ids = %v; want [7 8]", ids)
		}
	}
	if got := connections.Load(); got != 2 {
		t.Errorf("opened %d worker connections; want one per worker", got)
	}
}
