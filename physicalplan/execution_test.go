package physicalplan_test

import (
	"iter"
	"reflect"
	"testing"

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
			{Name: "quantity", Type: "integer"},
			{Name: "price", Type: "float"},
		}},
		Columns: [][]any{{2}, {9.5}},
	}}
	plan := physicalplan.ProjectionExec{
		Input:  physicalplan.ScanExec{Source: source},
		Schema: datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: "float"}}},
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
		Schema:  datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: "float"}}},
		Columns: [][]any{{9.5}},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute() = %#v; want %#v", got, want)
	}
}
