package worker_test

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gabrielelanaro/queng/batchserde"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/physicalplan"
	"github.com/gabrielelanaro/queng/planserde"
	"github.com/gabrielelanaro/queng/worker"
	"github.com/gabrielelanaro/queng/workerapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func makeExchangePlan(t *testing.T) *planserde.Plan {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inventory.csv")
	if err := os.WriteFile(path, []byte("id,price\n1,10\n2,20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewCSVSource(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planserde.SerializePlan(distributed.ExchangeWrite{Input: physicalplan.ScanExec{Source: source}, StageID: 3, KeyIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func startWorker(t *testing.T, store *distributed.MemoryExchange) workerapi.WorkerClient {
	t.Helper()
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
	return workerapi.NewWorkerClient(conn)
}

func TestRunTaskWritesExchangeOutput(t *testing.T) {
	plan := makeExchangePlan(t)
	store := distributed.NewMemoryExchange()
	t.Cleanup(func() { store.Close() })
	client := startWorker(t, store)

	_, err := client.RunTask(context.Background(), &workerapi.TaskRequest{JobId: "job-1", TaskId: 7, PartitionId: 0, ShufflePartitions: 2, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	var ids []int
	for partition := range 2 {
		key := execution.ExchangeKey{JobID: "job-1", StageID: 3, TaskID: 7, PartitionID: partition}
		for batch, err := range store.Read(key) {
			if err != nil {
				t.Fatal(err)
			}
			for row := range batch.Column(0).Len() {
				ids = append(ids, int(batch.Column(0).Value(row).(int64)))
			}
			batch.Release()
		}
	}
	sort.Ints(ids)
	if !reflect.DeepEqual(ids, []int{1, 2}) {
		t.Errorf("ids = %v; want [1 2]", ids)
	}

	_, err = client.RunTask(context.Background(), &workerapi.TaskRequest{
		JobId: "job-1", TaskId: 8, PartitionId: 1, ShufflePartitions: 2, Plan: plan,
	})
	if status.Code(err) != codes.Internal {
		t.Errorf("invalid input partition returned %v; want execution error", err)
	}
}

func TestRunTaskReturnsProjectedBatches(t *testing.T) {
	input, err := planserde.DeserializePlan(makeExchangePlan(t))
	if err != nil {
		t.Fatal(err)
	}
	projection := physicalplan.ProjectionExec{
		Input: input.(distributed.ExchangeWrite).Input,
		OutputSchema: datasource.Schema{Fields: []datasource.Field{
			{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Int64},
		}},
		Expr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 1}},
	}
	plan, err := planserde.SerializePlan(projection)
	if err != nil {
		t.Fatal(err)
	}
	store := distributed.NewMemoryExchange()
	t.Cleanup(func() { store.Close() })
	client := startWorker(t, store)
	result, err := client.RunTask(context.Background(), &workerapi.TaskRequest{
		JobId: "job-1", ShufflePartitions: 2, Plan: plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	var values []any
	for _, data := range result.Batches {
		batch, err := batchserde.Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(batch.Schema, projection.OutputSchema) {
			batch.Release()
			t.Fatal("returned batch has an unexpected schema")
		}
		values = append(values, batch.Column(0).Value(0))
		batch.Release()
	}
	if !reflect.DeepEqual(values, []any{int64(10), int64(20)}) {
		t.Errorf("values = %v; want [10 20]", values)
	}
}

func TestReadPartitionStreamsProducerBatches(t *testing.T) {
	store := distributed.NewMemoryExchange()
	t.Cleanup(func() { store.Close() })
	client := startWorker(t, store)
	writeExchangeBatch(t, store, 7, 1)
	writeExchangeBatch(t, store, 7, 2)
	writeExchangeBatch(t, store, 8, 3)

	stream, err := client.ReadPartition(context.Background(), &workerapi.PartitionRequest{
		JobId: "job-1", StageId: 3, TaskId: 7, PartitionId: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	var ids []any
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		batch, err := batchserde.Decode(message.Data)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, batch.Column(0).Value(0))
		batch.Release()
	}
	if !reflect.DeepEqual(ids, []any{int64(1), int64(2)}) {
		t.Errorf("ids = %v; want [1 2] from task 7", ids)
	}
}

func writeExchangeBatch(t *testing.T, store *distributed.MemoryExchange, taskID int, id int64) {
	t.Helper()
	column, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, []any{id})
	if err != nil {
		t.Fatal(err)
	}
	batch := datasource.NewRecordBatch(
		datasource.Schema{Fields: []datasource.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}},
		[]datasource.ColumnVector{column},
	)
	defer batch.Release()
	key := execution.ExchangeKey{JobID: "job-1", StageID: 3, TaskID: taskID, PartitionID: 0}
	if err := store.Write(key, batch); err != nil {
		t.Fatal(err)
	}
}
