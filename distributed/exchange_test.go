package distributed

import (
	"context"
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/physicalplan"
)

type exchangeTestSource struct{ batch datasource.RecordBatch }

func (s exchangeTestSource) Schema() datasource.Schema { return s.batch.Schema }
func (s exchangeTestSource) PartitionCount() int       { return 1 }
func (s exchangeTestSource) Scan(partitionID int, columns []int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		s.batch.Retain()
		yield(s.batch, nil)
	}
}

func makeSalesSource(t *testing.T) exchangeTestSource {
	t.Helper()
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "sale", Type: arrow.PrimitiveTypes.Float64},
	}}
	keys, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, []any{int64(1), int64(2), int64(1), int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(keys.Release)
	sales, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Float64, []any{10.0, 20.0, 30.0, 40.0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sales.Release)
	return exchangeTestSource{batch: datasource.NewRecordBatch(schema, []datasource.ColumnVector{keys, sales})}
}

func TestExchangeWritePartitionsRowsByKey(t *testing.T) {
	source := makeSalesSource(t)
	store := NewMemoryExchange()
	defer store.Close()
	writer := ExchangeWriteExec{
		Input:        physicalplan.ScanExec{Source: source},
		StageID:      0,
		Partitioning: HashPartitioning{Keys: []int{0}},
	}

	for batch, err := range writer.Execute(context.Background(), execution.Environment{JobID: "job-1", TaskID: 7, Exchange: store, ShufflePartitions: 2}, 0) {
		if err != nil {
			t.Fatalf("ExchangeWriteExec.Execute() returned an error: %v", err)
		}
		batch.Release()
		t.Fatal("ExchangeWriteExec.Execute() returned a batch; want exchange output only")
	}

	rows := make(map[int64][]any)
	partitions := make(map[int64]int)
	seenPartitions := make(map[int]bool)
	for partitionID := 0; partitionID < 2; partitionID++ {
		key := execution.ExchangeKey{JobID: "job-1", StageID: 0, TaskID: 7, PartitionID: partitionID}
		for batch, err := range store.Read(key) {
			if err != nil {
				t.Fatalf("exchange read returned an error: %v", err)
			}
			if !reflect.DeepEqual(batch.Schema, source.Schema()) {
				batch.Release()
				t.Fatalf("schema = %#v; want %#v", batch.Schema, source.Schema())
			}
			for row := 0; row < batch.Column(0).Len(); row++ {
				seenPartitions[partitionID] = true
				id := batch.Column(0).Value(row).(int64)
				if previous, ok := partitions[id]; ok && previous != partitionID {
					batch.Release()
					t.Fatalf("key %d was written to multiple partitions", id)
				}
				partitions[id] = partitionID
				rows[id] = append(rows[id], batch.Column(1).Value(row))
			}
			batch.Release()
		}
	}
	want := map[int64][]any{1: {10.0, 30.0}, 2: {20.0}, 3: {40.0}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("exchange rows = %#v; want %#v", rows, want)
	}
	if len(seenPartitions) != 2 {
		t.Errorf("rows were written to %d partitions; want 2", len(seenPartitions))
	}
}

func TestExchangeReadCollectsPartitionFromAllProducers(t *testing.T) {
	source := makeSalesSource(t)
	store := NewMemoryExchange()
	defer store.Close()
	keys := []execution.ExchangeKey{
		{JobID: "job-1", StageID: 1, TaskID: 10, PartitionID: 2},
		{JobID: "job-1", StageID: 1, TaskID: 11, PartitionID: 2},
		{JobID: "job-2", StageID: 1, TaskID: 10, PartitionID: 2},
		{JobID: "job-1", StageID: 3, TaskID: 10, PartitionID: 2},
		{JobID: "job-1", StageID: 1, TaskID: 10, PartitionID: 3},
	}
	for _, key := range keys {
		if err := store.Write(key, source.batch); err != nil {
			t.Fatal(err)
		}
	}

	reader := ExchangeReadExec{StageID: 1, OutputSchema: source.Schema(), Partitioning: HashPartitioning{Keys: []int{0}}}
	var batches int
	for batch, err := range reader.Execute(context.Background(), execution.Environment{JobID: "job-1", Exchange: store}, 2) {
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(batch.Schema, source.Schema()) {
			batch.Release()
			t.Fatal("unexpected batch schema")
		}
		batches++
		batch.Release()
	}
	if batches != 2 {
		t.Errorf("read %d batches; want 2 from the matching producers", batches)
	}
}

func TestSinglePartitionExchangeWritesAllRowsToPartitionZero(t *testing.T) {
	source := makeSalesSource(t)
	store := NewMemoryExchange()
	defer store.Close()
	writer := ExchangeWriteExec{
		Input:        physicalplan.ScanExec{Source: source},
		Partitioning: SinglePartition{},
	}
	// Multiple producers must gather into the same output partition.
	for taskID := range 2 {
		env := execution.Environment{JobID: "gather", TaskID: taskID, Exchange: store, ShufflePartitions: 3}
		for batch, err := range writer.Execute(context.Background(), env, 0) {
			batch.Release()
			if err != nil {
				t.Fatal(err)
			}
			t.Fatal("writer returned a batch")
		}
	}

	var sales []any
	for partition := range 3 {
		for batch, err := range store.ReadPartition(context.Background(), "gather", 0, partition) {
			if err != nil {
				t.Fatal(err)
			}
			if partition != 0 {
				batch.Release()
				t.Fatalf("gather wrote to partition %d; want only partition 0", partition)
			}
			for row := range batch.Column(1).Len() {
				sales = append(sales, batch.Column(1).Value(row))
			}
			batch.Release()
		}
	}
	want := []any{10.0, 20.0, 30.0, 40.0, 10.0, 20.0, 30.0, 40.0}
	if !reflect.DeepEqual(sales, want) {
		t.Errorf("gathered sales = %v; want %v", sales, want)
	}
}
