package executor_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/executor"
	"github.com/gabrielelanaro/queng/physicalplan"
	"github.com/gabrielelanaro/queng/worker"
	"github.com/gabrielelanaro/queng/workerapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func makeProjection(t *testing.T) physicalplan.ProjectionExec {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inventory.csv")
	if err := os.WriteFile(path, []byte("id,price\n1,10\n2,20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewCSVSource(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	return physicalplan.ProjectionExec{
		Input:        physicalplan.ScanExec{Source: source},
		OutputSchema: datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Int64}}},
		Expr:         []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 1}},
	}
}

func makeRemoteExecutor(t *testing.T) executor.RemoteExecutor {
	t.Helper()
	store := distributed.NewMemoryExchange()
	t.Cleanup(func() { store.Close() })
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	workerapi.RegisterWorkerServer(server, &worker.Server{Exchange: store})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///worker", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return executor.RemoteExecutor{Client: workerapi.NewWorkerClient(conn), ShufflePartitions: 2}
}

func TestRemoteExecutorReturnsTaskResults(t *testing.T) {
	remote := makeRemoteExecutor(t)
	task := executor.Task{JobID: "job-1", ID: 7, PartitionID: 0, Stage: distributed.Stage{Plan: makeProjection(t)}}
	result := <-remote.Submit(context.Background(), task, nil)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	var values []any
	for _, batch := range result.Batches {
		for row := range batch.Column(0).Len() {
			values = append(values, batch.Column(0).Value(row))
		}
		batch.Release()
	}
	if !reflect.DeepEqual(values, []any{int64(10), int64(20)}) {
		t.Errorf("values = %v; want [10 20]", values)
	}

	task.PartitionID = 1
	if result := <-remote.Submit(context.Background(), task, nil); result.Err == nil {
		t.Fatal("invalid partition succeeded; want an execution error")
	}
}
