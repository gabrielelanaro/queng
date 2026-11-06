package physicalplan_test

import (
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/physicalplan"
)

type batchesSource struct {
	schema  datasource.Schema
	batches []datasource.RecordBatch
}

type ownedBatchSource struct{ batch datasource.RecordBatch }

func (s ownedBatchSource) Schema() datasource.Schema { return s.batch.Schema }

func (s ownedBatchSource) PartitionCount() int { return 1 }

func (s ownedBatchSource) Scan(partitionID int) iter.Seq2[datasource.RecordBatch, error] {
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
	source := ownedBatchSource{batch: datasource.NewRecordBatch(schema, []datasource.ColumnVector{column})}
	plan := physicalplan.ProjectionExec{
		Input: physicalplan.ScanExec{Source: source}, OutputSchema: schema,
		Expr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
	}

	for batch, err := range plan.Execute(execution.Context{}, 0) {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if got := batch.Column(0).Value(0); got != 9.5 {
			t.Errorf("projected value = %v; want 9.5", got)
		}
		batch.Release()
	}
	allocator.AssertSize(t, 0)
}

func (s batchesSource) Schema() datasource.Schema { return s.schema }

// Each yielded batch owns references that the consumer must release.
func (s batchesSource) PartitionCount() int { return 1 }

func (s batchesSource) Scan(partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		for _, batch := range s.batches {
			batch.Retain()
			if !yield(batch, nil) {
				return
			}
		}
	}
}

func makeBatch(t *testing.T, schema datasource.Schema, values ...[]any) datasource.RecordBatch {
	t.Helper()
	var columns []datasource.ColumnVector
	for i, values := range values {
		column, err := datasource.NewArrowColumn(schema.Fields[i].Type, values)
		if err != nil {
			datasource.NewRecordBatch(schema, columns).Release()
			t.Fatalf("NewArrowColumn() returned an error: %v", err)
		}
		columns = append(columns, column)
	}
	batch := datasource.NewRecordBatch(schema, columns)
	t.Cleanup(batch.Release)
	return batch
}

func TestScanExecReturnsQualifiedSchema(t *testing.T) {
	sourceSchema := datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}}
	outputSchema := datasource.Schema{Fields: []datasource.Field{{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Float64}}}
	source := batchesSource{schema: sourceSchema, batches: []datasource.RecordBatch{
		makeBatch(t, sourceSchema, []any{9.5}),
	}}
	plan := physicalplan.ScanExec{Source: source, OutputSchema: outputSchema}
	if !reflect.DeepEqual(plan.Schema(), outputSchema) {
		t.Fatalf("Schema() = %#v; want %#v", plan.Schema(), outputSchema)
	}
	for batch, err := range plan.Execute(execution.Context{}, 0) {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if !reflect.DeepEqual(batch.Schema, plan.Schema()) {
			batch.Release()
			t.Fatalf("batch schema = %#v; want %#v", batch.Schema, plan.Schema())
		}
		batch.Release()
	}
}

func TestProjectionExecReturnsSelectedColumn(t *testing.T) {
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	batch := makeBatch(t, schema, []any{int64(2)}, []any{9.5})
	source := batchesSource{schema: schema, batches: []datasource.RecordBatch{batch}}
	plan := physicalplan.ProjectionExec{
		Input:        physicalplan.ScanExec{Source: source},
		OutputSchema: datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}},
		Expr: []physicalplan.PhysicalExpr{
			physicalplan.ColumnExpr{Index: 1},
		},
	}

	var values []any
	for result, err := range plan.Execute(execution.Context{}, 0) {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if !reflect.DeepEqual(result.Schema, plan.Schema()) {
			result.Release()
			t.Fatalf("batch schema = %#v; want %#v", result.Schema, plan.Schema())
		}
		for i := 0; i < result.Column(0).Len(); i++ {
			values = append(values, result.Column(0).Value(i))
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
		OutputSchema: datasource.Schema{Fields: []datasource.Field{
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "AVG", Type: arrow.PrimitiveTypes.Float64},
		}},
		GroupExpr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
		AggregateExpr: []physicalplan.AggregateExpr{
			physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 1}},
		},
	}

	got := map[any]any{}
	for batch, err := range plan.Execute(execution.Context{}, 0) {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if !reflect.DeepEqual(batch.Schema, plan.Schema()) {
			batch.Release()
			t.Fatalf("batch schema = %#v; want %#v", batch.Schema, plan.Schema())
		}
		for i := 0; i < batch.Column(0).Len(); i++ {
			got[batch.Column(0).Value(i)] = batch.Column(1).Value(i)
		}
		batch.Release()
	}
	want := map[any]any{"fruit": 6.0, "books": 15.0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("grouped averages = %#v; want %#v", got, want)
	}
}

func TestHashJoinExecMatchesAndPreservesUnmatchedLeftRows(t *testing.T) {
	leftSchema := datasource.Schema{Fields: []datasource.Field{
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "sale", Type: arrow.PrimitiveTypes.Float64},
	}}
	rightSchema := datasource.Schema{Fields: []datasource.Field{
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "product", Type: arrow.BinaryTypes.String},
	}}
	left := batchesSource{schema: leftSchema, batches: []datasource.RecordBatch{
		makeBatch(t, leftSchema, []any{int64(1), int64(2)}, []any{10.0, 20.0}),
		makeBatch(t, leftSchema, []any{nil}, []any{30.0}),
	}}
	right := batchesSource{schema: rightSchema, batches: []datasource.RecordBatch{
		makeBatch(t, rightSchema, []any{int64(1), nil}, []any{"apple", "unknown"}),
		makeBatch(t, rightSchema, []any{int64(1)}, []any{"pear"}),
	}}
	plan := physicalplan.HashJoinExec{
		Left: physicalplan.ScanExec{Source: left}, Right: physicalplan.ScanExec{Source: right},
		Type: logicalplan.LeftJoin, LeftKeys: []int{0}, RightKeys: []int{0},
		OutputSchema: datasource.Schema{Fields: append(leftSchema.Fields, rightSchema.Fields...)},
	}

	var rows [][]any
	for batch, err := range plan.Execute(execution.Context{}, 0) {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		if !reflect.DeepEqual(batch.Schema, plan.Schema()) {
			batch.Release()
			t.Fatal("output schema does not match join schema")
		}
		for i := 0; i < batch.Column(0).Len(); i++ {
			var row []any
			for index := 0; index < batch.ColumnCount(); index++ {
				row = append(row, batch.Column(index).Value(i))
			}
			rows = append(rows, row)
		}
		batch.Release()
	}
	want := [][]any{
		{int64(1), 10.0, int64(1), "apple"},
		{int64(1), 10.0, int64(1), "pear"},
		{int64(2), 20.0, nil, nil},
		{nil, 30.0, nil, nil},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("joined rows = %#v; want %#v", rows, want)
	}
}

func TestHashJoinExecRejectsMultipleKeys(t *testing.T) {
	plan := physicalplan.HashJoinExec{
		Type: logicalplan.LeftJoin, LeftKeys: []int{0, 1}, RightKeys: []int{0, 1},
	}
	for _, err := range plan.Execute(execution.Context{}, 0) {
		if err == nil {
			t.Fatal("Execute() succeeded with multiple join keys")
		}
		return
	}
	t.Fatal("Execute() did not report an error for multiple join keys")
}
