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

func (s testSource) PartitionCount() int { return 1 }

func (s testSource) Scan(partitionID int, columns []int) iter.Seq2[datasource.RecordBatch, error] {
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
		Input: physicalplan.ScanExec{Source: source, OutputSchema: datasource.Schema{Fields: []datasource.Field{
			{Qualifier: "inventory", Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
			{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Float64},
			{Qualifier: "inventory", Name: "name", Type: arrow.BinaryTypes.String},
		}}},
		OutputSchema: datasource.Schema{Fields: []datasource.Field{{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Float64}}},
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
			Input: physicalplan.ScanExec{Source: source, OutputSchema: datasource.Schema{Fields: []datasource.Field{
				{Qualifier: "inventory", Name: "category", Type: arrow.BinaryTypes.String},
				{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Float64},
			}}},
			OutputSchema: datasource.Schema{Fields: []datasource.Field{
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
		OutputSchema: datasource.Schema{Fields: []datasource.Field{{Name: "AVG", Type: arrow.PrimitiveTypes.Float64}}},
		Expr: []physicalplan.PhysicalExpr{
			physicalplan.ColumnExpr{Index: 1},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildPhysicalPlan() = %#v; want %#v", got, want)
	}
}

func TestBuildPhysicalPlanForLeftJoin(t *testing.T) {
	sales := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "sale", Type: arrow.PrimitiveTypes.Float64},
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
	}}}
	products := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "product", Type: arrow.BinaryTypes.String},
	}}}
	logical := logicalplan.Join{
		Left:  logicalplan.Scan{TableName: "sales", Source: sales},
		Right: logicalplan.Scan{TableName: "products", Source: products},
		Type:  logicalplan.LeftJoin,
		On:    []logicalplan.JoinKey{{Left: "product_id", Right: "product_id"}},
	}

	got, err := physicalplan.BuildPhysicalPlan(logical)
	if err != nil {
		t.Fatalf("BuildPhysicalPlan() returned an error: %v", err)
	}

	want := physicalplan.HashJoinExec{
		Left: physicalplan.ScanExec{Source: sales, OutputSchema: datasource.Schema{Fields: []datasource.Field{
			{Qualifier: "sales", Name: "sale", Type: arrow.PrimitiveTypes.Float64},
			{Qualifier: "sales", Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		}}},
		Right: physicalplan.ScanExec{Source: products, OutputSchema: datasource.Schema{Fields: []datasource.Field{
			{Qualifier: "products", Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
			{Qualifier: "products", Name: "product", Type: arrow.BinaryTypes.String},
		}}},
		Type:      logicalplan.LeftJoin,
		LeftKeys:  []int{1},
		RightKeys: []int{0},
		OutputSchema: datasource.Schema{Fields: []datasource.Field{
			{Qualifier: "sales", Name: "sale", Type: arrow.PrimitiveTypes.Float64},
			{Qualifier: "sales", Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
			{Qualifier: "products", Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
			{Qualifier: "products", Name: "product", Type: arrow.BinaryTypes.String},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildPhysicalPlan() = %#v; want %#v", got, want)
	}
}

func TestBuildPhysicalPlanResolvesColumnsAgainstSelectedScanSchema(t *testing.T) {
	source := testSource{schema: datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}}}
	logical := logicalplan.Projection{
		Input: logicalplan.Scan{TableName: "inventory", Source: source, Columns: []string{"price", "quantity"}},
		Expr:  []logicalplan.LogicalExpr{logicalplan.Column{Name: "price"}},
	}

	got, err := physicalplan.BuildPhysicalPlan(logical)
	if err != nil {
		t.Fatal(err)
	}
	selectedSchema := datasource.Schema{Fields: []datasource.Field{
		{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Float64},
		{Qualifier: "inventory", Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
	}}
	want := physicalplan.ProjectionExec{
		Input:        physicalplan.ScanExec{Source: source, Columns: []int{1, 0}, OutputSchema: selectedSchema},
		OutputSchema: datasource.Schema{Fields: selectedSchema.Fields[:1]},
		Expr:         []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildPhysicalPlan() = %#v; want %#v", got, want)
	}
}
