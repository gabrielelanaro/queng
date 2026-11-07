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
func (s exchangeTestSource) Scan(partitionID int) iter.Seq2[datasource.RecordBatch, error] {
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
	writer := ExchangeWrite{
		Input:   physicalplan.ScanExec{Source: source},
		StageID: 0,
	}

	for batch, err := range writer.Execute(execution.Context{JobID: "job-1", TaskID: 7, Exchange: store, ShufflePartitions: 2}, 0) {
		if err != nil {
			t.Fatalf("ExchangeWrite.Execute() returned an error: %v", err)
		}
		batch.Release()
		t.Fatal("ExchangeWrite.Execute() returned a batch; want exchange output only")
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

	reader := ExchangeRead{StageID: 1, OutputSchema: source.Schema()}
	var batches int
	for batch, err := range reader.Execute(execution.Context{Context: context.Background(), JobID: "job-1", Exchange: store}, 2) {
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
