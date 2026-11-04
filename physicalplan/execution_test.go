package physicalplan_test

import (
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/physicalplan"
)

type batchesSource struct {
	schema  datasource.Schema
	batches []datasource.RecordBatch
}

type ownedBatchSource struct{ batch datasource.RecordBatch }

func (s ownedBatchSource) Schema() datasource.Schema { return s.batch.Schema }

func (s ownedBatchSource) Scan() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		yield(s.batch, nil)
	}
}

func TestProjectionKeepsColumnAliveUntilOutputIsReleased(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewFloat64Builder(allocator)
	builder.Append(9.5)
	column := datasource.ArrowColumn{Array: builder.NewArray()}
	builder.Release()
	schema := datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}}
	source := ownedBatchSource{batch: datasource.RecordBatch{Schema: schema, Columns: []datasource.ColumnVector{column}}}
	plan := physicalplan.ProjectionExec{
		Input: physicalplan.ScanExec{Source: source}, Schema: schema,
		Expr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
	}

	for batch, err := range plan.Execute() {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if got := batch.Columns[0].Value(0); got != 9.5 {
			t.Errorf("projected value = %v; want 9.5", got)
		}
		batch.Release()
	}
	allocator.AssertSize(t, 0)
}

func (s batchesSource) Schema() datasource.Schema { return s.schema }

// Each yielded batch owns references that the consumer must release.
func (s batchesSource) Scan() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		for _, batch := range s.batches {
			for _, column := range batch.Columns {
				column.Retain()
			}
			if !yield(batch, nil) {
				return
			}
		}
	}
}

func makeBatch(t *testing.T, schema datasource.Schema, values ...[]any) datasource.RecordBatch {
	t.Helper()
	batch := datasource.RecordBatch{Schema: schema}
	for i, values := range values {
		column, err := datasource.NewArrowColumn(schema.Fields[i].Type, values)
		if err != nil {
			batch.Release()
			t.Fatalf("NewArrowColumn() returned an error: %v", err)
		}
		batch.Columns = append(batch.Columns, column)
	}
	t.Cleanup(batch.Release)
	return batch
}

func TestProjectionExecReturnsSelectedColumn(t *testing.T) {
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	batch := makeBatch(t, schema, []any{int64(2)}, []any{9.5})
	source := batchesSource{schema: schema, batches: []datasource.RecordBatch{batch}}
	plan := physicalplan.ProjectionExec{
		Input:  physicalplan.ScanExec{Source: source},
		Schema: datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}},
		Expr: []physicalplan.PhysicalExpr{
			physicalplan.ColumnExpr{Index: 1},
		},
	}

	var values []any
	for result, err := range plan.Execute() {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if !reflect.DeepEqual(result.Schema, plan.Schema) {
			result.Release()
			t.Fatalf("batch schema = %#v; want %#v", result.Schema, plan.Schema)
		}
		for i := 0; i < result.Columns[0].Len(); i++ {
			values = append(values, result.Columns[0].Value(i))
		}
		result.Release()
	}
	if !reflect.DeepEqual(values, []any{9.5}) {
		t.Errorf("projected values = %#v; want [9.5]", values)
	}
}

func TestHashAggregateExecAveragesEachGroupAcrossBatches(t *testing.T) {
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	source := batchesSource{schema: schema, batches: []datasource.RecordBatch{
		makeBatch(t, schema, []any{"fruit", "books"}, []any{4.0, 10.0}),
		makeBatch(t, schema, []any{"fruit", "books"}, []any{8.0, 20.0}),
	}}
	plan := physicalplan.HashAggregateExec{
		Input: physicalplan.ScanExec{Source: source},
		Schema: datasource.Schema{Fields: []datasource.Field{
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "AVG", Type: arrow.PrimitiveTypes.Float64},
		}},
		GroupExpr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
		AggregateExpr: []physicalplan.AggregateExpr{
			physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 1}},
		},
	}

	got := map[any]any{}
	for batch, err := range plan.Execute() {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if !reflect.DeepEqual(batch.Schema, plan.Schema) {
			batch.Release()
			t.Fatalf("batch schema = %#v; want %#v", batch.Schema, plan.Schema)
		}
		for i := 0; i < batch.Columns[0].Len(); i++ {
			got[batch.Columns[0].Value(i)] = batch.Columns[1].Value(i)
		}
		batch.Release()
	}
	want := map[any]any{"fruit": 6.0, "books": 15.0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("grouped averages = %#v; want %#v", got, want)
	}
}
