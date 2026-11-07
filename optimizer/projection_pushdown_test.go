package optimizer_test

import (
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/optimizer"
)

type schemaSource struct{ schema datasource.Schema }

func (s schemaSource) Schema() datasource.Schema { return s.schema }
func (schemaSource) PartitionCount() int         { return 1 }
func (schemaSource) Scan(int, []int) iter.Seq2[datasource.RecordBatch, error] {
	return func(func(datasource.RecordBatch, error) bool) {}
}

func makeScan(name string, columns ...string) logicalplan.Scan {
	schema := datasource.Schema{}
	for _, column := range columns {
		schema.Fields = append(schema.Fields, datasource.Field{Name: column, Type: arrow.PrimitiveTypes.Int64})
	}
	return logicalplan.Scan{TableName: name, Source: schemaSource{schema: schema}}
}

func optimize(t *testing.T, plan logicalplan.LogicalPlan) logicalplan.LogicalPlan {
	t.Helper()
	got, err := optimizer.New(optimizer.ProjectionPushDownRule{}).Optimize(plan)
	if err != nil {
		t.Fatal(err)
	}
	before, err := plan.Schema()
	if err != nil {
		t.Fatal(err)
	}
	after, err := got.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("output schema changed: before %#v, after %#v", before, after)
	}
	return got
}

func TestProjectionPushDownSelectsOnlyProjectedScanColumns(t *testing.T) {
	scan := makeScan("inventory", "quantity", "price", "unused")
	plan := logicalplan.Projection{
		Input: scan,
		Expr:  []logicalplan.LogicalExpr{logicalplan.Column{Name: "price"}},
	}

	got := optimize(t, plan)
	scan.Columns = []string{"price"}
	want := logicalplan.Projection{Input: scan, Expr: plan.Expr}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("optimized plan = %#v; want %#v", got, want)
	}
}

func TestProjectionPushDownKeepsGroupingAndAggregateInputs(t *testing.T) {
	scan := makeScan("inventory", "unused", "category", "price", "quantity")
	aggregate := logicalplan.Aggregate{
		Input:     scan,
		GroupExpr: []logicalplan.LogicalExpr{logicalplan.ColumnIndex{Index: 1}},
		AggregateExpr: []logicalplan.AggregateExpr{
			logicalplan.Avg{Expr: logicalplan.ColumnIndex{Index: 2}},
			logicalplan.Avg{Expr: logicalplan.ColumnIndex{Index: 3}},
		},
	}
	// Select the second average; both aggregate outputs are named AVG.
	plan := logicalplan.Projection{
		Input: aggregate,
		Expr:  []logicalplan.LogicalExpr{logicalplan.ColumnIndex{Index: 2}},
	}

	got := optimize(t, plan)
	scan.Columns = []string{"category", "price", "quantity"}
	want := logicalplan.Projection{
		Input: logicalplan.Aggregate{
			Input:     scan,
			GroupExpr: []logicalplan.LogicalExpr{logicalplan.ColumnIndex{Index: 0}},
			AggregateExpr: []logicalplan.AggregateExpr{
				logicalplan.Avg{Expr: logicalplan.ColumnIndex{Index: 1}},
				logicalplan.Avg{Expr: logicalplan.ColumnIndex{Index: 2}},
			},
		},
		Expr: plan.Expr,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("optimized plan = %#v; want %#v", got, want)
	}
}

func TestProjectionPushDownKeepsJoinKeysAndRemapsOutputPositions(t *testing.T) {
	left := makeScan("sales", "unused", "product_id", "price")
	right := makeScan("products", "unused", "product_id", "price")
	join := logicalplan.Join{
		Left: left, Right: right, Type: logicalplan.LeftJoin,
		On: []logicalplan.JoinKey{{Left: "product_id", Right: "product_id"}},
	}
	plan := logicalplan.Projection{
		Input: join,
		Expr:  []logicalplan.LogicalExpr{logicalplan.ColumnIndex{Index: 5}},
	}

	got := optimize(t, plan)
	left.Columns = []string{"product_id"}
	right.Columns = []string{"product_id", "price"}
	join.Left, join.Right = left, right
	want := logicalplan.Projection{
		Input: join,
		Expr:  []logicalplan.LogicalExpr{logicalplan.ColumnIndex{Index: 2}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("optimized plan = %#v; want %#v", got, want)
	}
}

func TestProjectionPushDownKeepsQualifiedJoinColumnsSeparate(t *testing.T) {
	left := makeScan("sales", "product_id", "price", "unused")
	right := makeScan("products", "product_id", "price", "unused")
	join := logicalplan.Join{
		Left: left, Right: right, Type: logicalplan.LeftJoin,
		On: []logicalplan.JoinKey{{Left: "product_id", Right: "product_id"}},
	}
	plan := logicalplan.Projection{
		Input: join,
		Expr:  []logicalplan.LogicalExpr{logicalplan.Column{Qualifier: "products", Name: "price"}},
	}

	got := optimize(t, plan)
	left.Columns = []string{"product_id"}
	right.Columns = []string{"product_id", "price"}
	join.Left, join.Right = left, right
	want := logicalplan.Projection{Input: join, Expr: plan.Expr}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("optimized plan = %#v; want %#v", got, want)
	}
}

func TestProjectionPushDownCanBeRepeated(t *testing.T) {
	plan := logicalplan.Projection{
		Input: makeScan("inventory", "unused", "price"),
		Expr:  []logicalplan.LogicalExpr{logicalplan.ColumnIndex{Index: 1}},
	}
	once := optimize(t, plan)
	twice := optimize(t, once)
	if !reflect.DeepEqual(twice, once) {
		t.Errorf("second optimization changed the plan: %#v; first %#v", twice, once)
	}
	if plan.Expr[0] != (logicalplan.ColumnIndex{Index: 1}) {
		t.Fatal("optimization modified the original projection")
	}
}
