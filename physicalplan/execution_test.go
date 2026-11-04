package physicalplan_test

import (
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/physicalplan"
)

type batchSource struct {
	batch datasource.RecordBatch
}

func (s batchSource) Schema() datasource.Schema { return s.batch.Schema }

func (s batchSource) Scan() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		yield(s.batch, nil)
	}
}

func TestProjectionExecReturnsSelectedColumn(t *testing.T) {
	source := batchSource{batch: datasource.RecordBatch{
		Schema: datasource.Schema{Fields: []datasource.Field{
			{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
			{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		}},
		Columns: [][]any{{2}, {9.5}},
	}}
	plan := physicalplan.ProjectionExec{
		Input:  physicalplan.ScanExec{Source: source},
		Schema: datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}},
		Expr: []physicalplan.PhysicalExpr{
			physicalplan.ColumnExpr{Index: 1},
		},
	}

	var got []datasource.RecordBatch
	for batch, err := range plan.Execute() {
		if err != nil {
			t.Fatalf("Execute() returned an error: %v", err)
		}
		got = append(got, batch)
	}
	want := []datasource.RecordBatch{{
		Schema:  datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}},
		Columns: [][]any{{9.5}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute() = %#v; want %#v", got, want)
	}
}

type batchesSource struct {
	schema  datasource.Schema
	batches []datasource.RecordBatch
}

func (s batchesSource) Schema() datasource.Schema { return s.schema }

func (s batchesSource) Scan() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		for _, batch := range s.batches {
			if !yield(batch, nil) {
				return
			}
		}
	}
}

func TestHashAggregateExecAveragesEachGroupAcrossBatches(t *testing.T) {
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	source := batchesSource{schema: schema, batches: []datasource.RecordBatch{
		{Schema: schema, Columns: [][]any{{"fruit", "books"}, {4.0, 10.0}}},
		{Schema: schema, Columns: [][]any{{"fruit", "books"}, {8.0, 20.0}}},
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
			t.Fatalf("batch schema = %#v; want %#v", batch.Schema, plan.Schema)
		}
		for i, category := range batch.Columns[0] {
			got[category] = batch.Columns[1][i]
		}
	}
	want := map[any]any{"fruit": 6.0, "books": 15.0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("grouped averages = %#v; want %#v", got, want)
	}
}
