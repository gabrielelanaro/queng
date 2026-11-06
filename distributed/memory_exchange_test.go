package distributed

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
)

func TestMemoryExchangeStoresBatchesByJobAndStage(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues([]int64{1, 2}, nil)
	column := datasource.ArrowColumn{Array: builder.NewArray()}
	builder.Release()
	batch := datasource.NewRecordBatch(
		datasource.Schema{Fields: []datasource.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}},
		[]datasource.ColumnVector{column},
	)
	store := NewMemoryExchange()
	key := execution.ExchangeKey{JobID: "job-1", StageID: 0, TaskID: 1, PartitionID: 2}
	if err := store.Write(key, batch); err != nil {
		batch.Release()
		t.Fatal(err)
	}
	batch.Release()

	var values []any
	for got, err := range store.Read(key) {
		if err != nil {
			t.Fatal(err)
		}
		for row := 0; row < got.Column(0).Len(); row++ {
			values = append(values, got.Column(0).Value(row))
		}
		got.Release()
	}
	if !reflect.DeepEqual(values, []any{int64(1), int64(2)}) {
		t.Errorf("values = %#v; want [1 2]", values)
	}
	key.JobID = "job-2"
	for got, err := range store.Read(key) {
		if err != nil {
			t.Fatal(err)
		}
		got.Release()
		t.Fatal("another job received the stored batch")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	allocator.AssertSize(t, 0)
}
