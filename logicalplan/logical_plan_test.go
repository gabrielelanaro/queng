package logicalplan_test

import (
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/parser"
)

type testSource struct{ schema datasource.Schema }

func (s testSource) Schema() datasource.Schema { return s.schema }

func (s testSource) Scan() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {}
}

func TestBuildLogicalPlan(t *testing.T) {
	query := parser.SqlSelect{
		Projection: []parser.SqlExpr{
			parser.SqlIdentifier{ID: "price"},
		},
		TableName: parser.SqlIdentifier{ID: "inventory"},
	}
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}}

	got, err := logicalplan.BuildLogicalPlan(query, map[string]datasource.DataSource{"inventory": source})
	if err != nil {
		t.Fatalf("BuildLogicalPlan() returned an error: %v", err)
	}

	want := logicalplan.Projection{
		Input: logicalplan.Scan{TableName: "inventory", Source: source},
		Expr: []logicalplan.LogicalExpr{
			logicalplan.Column{Name: "price"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildLogicalPlan() = %#v; want %#v", got, want)
	}
}

func TestProjectionSchemaReportsSelectedColumns(t *testing.T) {
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}}
	plan := logicalplan.Projection{
		Input: logicalplan.Scan{TableName: "inventory", Source: source},
		Expr: []logicalplan.LogicalExpr{
			logicalplan.Column{Name: "price"},
		},
	}

	schema, err := plan.Schema()
	if err != nil {
		t.Fatalf("Schema() returned an error: %v", err)
	}
	wantSchema := datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}}
	if !reflect.DeepEqual(schema, wantSchema) {
		t.Errorf("Schema() = %#v; want %#v", schema, wantSchema)
	}
}

func TestBuildLogicalPlanRejectsUnknownTableAndColumn(t *testing.T) {
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}}}
	tables := map[string]datasource.DataSource{"inventory": source}
	for _, tc := range []struct{ table, column string }{
		{"missing", "price"},
		{"inventory", "missing"},
	} {
		t.Run(tc.table+"/"+tc.column, func(t *testing.T) {
			query := parser.SqlSelect{
				Projection: []parser.SqlExpr{parser.SqlIdentifier{ID: tc.column}},
				TableName:  parser.SqlIdentifier{ID: tc.table},
			}
			if _, err := logicalplan.BuildLogicalPlan(query, tables); err == nil {
				t.Fatal("BuildLogicalPlan() succeeded; want an error")
			}
		})
	}
}

func TestBuildLogicalPlanForGroupedAverage(t *testing.T) {
	query := parser.SqlSelect{
		Projection: []parser.SqlExpr{
			parser.SqlFunction{
				ID: "AVG",
				Args: []parser.SqlExpr{
					parser.SqlIdentifier{ID: "price"},
				},
			},
		},
		TableName: parser.SqlIdentifier{ID: "inventory"},
		GroupBy: []parser.SqlExpr{
			parser.SqlIdentifier{ID: "category"},
		},
	}
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}}

	got, err := logicalplan.BuildLogicalPlan(query, map[string]datasource.DataSource{"inventory": source})
	if err != nil {
		t.Fatalf("BuildLogicalPlan() returned an error: %v", err)
	}

	want := logicalplan.Projection{
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
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildLogicalPlan() = %#v; want %#v", got, want)
	}
}

func TestAverageOfIntegersHasFloatSchema(t *testing.T) {
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
	}}}
	field, err := (logicalplan.Avg{Expr: logicalplan.Column{Name: "quantity"}}).ToField(
		logicalplan.Scan{TableName: "inventory", Source: source},
	)
	if err != nil {
		t.Fatalf("ToField() returned an error: %v", err)
	}
	if !arrow.TypeEqual(field.Type, arrow.PrimitiveTypes.Float64) {
		t.Errorf("average type = %v; want float64", field.Type)
	}
}
