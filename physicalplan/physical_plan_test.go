package physicalplan_test

import (
	"iter"
	"reflect"
	"testing"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/physicalplan"
)

type testSource struct{ schema datasource.Schema }

func (s testSource) Schema() datasource.Schema { return s.schema }

func (s testSource) Scan() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {}
}

func TestBuildPhysicalPlan(t *testing.T) {
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: "integer"},
		{Name: "price", Type: "float"},
		{Name: "name", Type: "string"},
	}}}
	logical := logicalplan.Projection{
		Input: logicalplan.Scan{TableName: "inventory", Source: source},
		Expr: []logicalplan.LogicalExpr{
			logicalplan.Column{Name: "price"},
		},
	}
	got, err := physicalplan.BuildPhysicalPlan(logical)
	if err != nil {
		t.Fatalf("BuildPhysicalPlan() returned an error: %v", err)
	}

	want := physicalplan.ProjectionExec{
		Input: physicalplan.ScanExec{Source: source},
		Expr: []physicalplan.PhysicalExpr{
			physicalplan.ColumnExpr{Index: 1},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildPhysicalPlan() = %#v; want %#v", got, want)
	}
}
