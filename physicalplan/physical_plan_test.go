package physicalplan_test

import (
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

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
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "name", Type: arrow.BinaryTypes.String},
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
		Input:  physicalplan.ScanExec{Source: source},
		Schema: datasource.Schema{Fields: []datasource.Field{{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Float64}}},
		Expr: []physicalplan.PhysicalExpr{
			physicalplan.ColumnExpr{Index: 1},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildPhysicalPlan() = %#v; want %#v", got, want)
	}
}

func TestBuildPhysicalPlanForGroupedAverage(t *testing.T) {
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}}
	logical := logicalplan.Projection{
		Input: logicalplan.Aggregate{
			Input: logicalplan.Scan{TableName: "inventory", Source: source},
			GroupExpr: []logicalplan.LogicalExpr{
				logicalplan.Column{Name: "category"},
			},
			AggregateExpr: []logicalplan.AggregateExpr{
				logicalplan.Avg{Expr: logicalplan.Column{Name: "price"}},
			},
		},
		Expr: []logicalplan.LogicalExpr{
			logicalplan.ColumnIndex{Index: 1},
		},
	}

	got, err := physicalplan.BuildPhysicalPlan(logical)
	if err != nil {
		t.Fatalf("BuildPhysicalPlan() returned an error: %v", err)
	}

	want := physicalplan.ProjectionExec{
		Input: physicalplan.HashAggregateExec{
			Input: physicalplan.ScanExec{Source: source},
			Schema: datasource.Schema{Fields: []datasource.Field{
				{Qualifier: "inventory", Name: "category", Type: arrow.BinaryTypes.String},
				{Name: "AVG", Type: arrow.PrimitiveTypes.Float64},
			}},
			GroupExpr: []physicalplan.PhysicalExpr{
				physicalplan.ColumnExpr{Index: 0},
			},
			AggregateExpr: []physicalplan.AggregateExpr{
				physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 1}},
			},
		},
		Schema: datasource.Schema{Fields: []datasource.Field{{Name: "AVG", Type: arrow.PrimitiveTypes.Float64}}},
		Expr: []physicalplan.PhysicalExpr{
			physicalplan.ColumnExpr{Index: 1},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildPhysicalPlan() = %#v; want %#v", got, want)
	}
}
