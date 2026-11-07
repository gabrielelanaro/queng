package distributed

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

func (s testSource) Scan(partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {}
}

func TestBuildStagesForHashJoin(t *testing.T) {
	salesSchema := datasource.Schema{Fields: []datasource.Field{
		{Qualifier: "sales", Name: "sale", Type: arrow.PrimitiveTypes.Float64},
		{Qualifier: "sales", Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
	}}
	productsSchema := datasource.Schema{Fields: []datasource.Field{
		{Qualifier: "products", Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		{Qualifier: "products", Name: "product", Type: arrow.BinaryTypes.String},
	}}
	sales := physicalplan.ScanExec{Source: testSource{schema: salesSchema}}
	products := physicalplan.ScanExec{Source: testSource{schema: productsSchema}}
	joinSchema := datasource.Schema{Fields: append(append([]datasource.Field{}, salesSchema.Fields...), productsSchema.Fields...)}
	plan := physicalplan.HashJoinExec{
		Left: sales, Right: products, Type: logicalplan.LeftJoin,
		LeftKeys: []int{1}, RightKeys: []int{0}, OutputSchema: joinSchema,
	}

	got, err := BuildStages(plan)
	if err != nil {
		t.Fatalf("BuildStages() returned an error: %v", err)
	}

	want := []Stage{
		{
			ID: 0,
			Plan: ExchangeWriteExec{
				Input: sales, KeyIndex: 1,
			},
		},
		{
			ID: 1,
			Plan: ExchangeWriteExec{
				Input: products, StageID: 1, KeyIndex: 0,
			},
		},
		{
			ID: 2, Dependencies: []int{0, 1},
			Plan: physicalplan.HashJoinExec{
				Left:         ExchangeReadExec{StageID: 0, OutputSchema: salesSchema},
				Right:        ExchangeReadExec{StageID: 1, OutputSchema: productsSchema},
				Type:         logicalplan.LeftJoin,
				LeftKeys:     []int{1},
				RightKeys:    []int{0},
				OutputSchema: joinSchema,
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildStages() = %#v; want %#v", got, want)
	}
}

func TestBuildStagesForNestedJoin(t *testing.T) {
	schema := datasource.Schema{Fields: []datasource.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}}
	scan := physicalplan.ScanExec{Source: testSource{schema: schema}}
	innerSchema := datasource.Schema{Fields: []datasource.Field{schema.Fields[0], schema.Fields[0]}}
	outerSchema := datasource.Schema{Fields: []datasource.Field{schema.Fields[0], schema.Fields[0], schema.Fields[0]}}
	inner := physicalplan.HashJoinExec{
		Left: scan, Right: scan, Type: logicalplan.LeftJoin,
		LeftKeys: []int{0}, RightKeys: []int{0}, OutputSchema: innerSchema,
	}
	outer := physicalplan.HashJoinExec{
		Left: inner, Right: scan, Type: logicalplan.LeftJoin,
		LeftKeys: []int{0}, RightKeys: []int{0}, OutputSchema: outerSchema,
	}
	projection := physicalplan.ProjectionExec{
		Input: outer, OutputSchema: schema, Expr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
	}

	got, err := BuildStages(projection)
	if err != nil {
		t.Fatalf("BuildStages() returned an error: %v", err)
	}
	inner.Left = ExchangeReadExec{StageID: 0, OutputSchema: schema}
	inner.Right = ExchangeReadExec{StageID: 1, OutputSchema: schema}
	outer.Left = ExchangeReadExec{StageID: 2, OutputSchema: innerSchema}
	outer.Right = ExchangeReadExec{StageID: 3, OutputSchema: schema}
	projection.Input = outer
	want := []Stage{
		{ID: 0, Plan: ExchangeWriteExec{Input: scan, KeyIndex: 0}},
		{ID: 1, Plan: ExchangeWriteExec{Input: scan, StageID: 1, KeyIndex: 0}},
		{ID: 2, Plan: ExchangeWriteExec{Input: inner, StageID: 2, KeyIndex: 0}, Dependencies: []int{0, 1}},
		{ID: 3, Plan: ExchangeWriteExec{Input: scan, StageID: 3, KeyIndex: 0}},
		{ID: 4, Plan: projection, Dependencies: []int{2, 3}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildStages() = %#v; want %#v", got, want)
	}
}

func TestBuildStagesForGroupedAverage(t *testing.T) {
	inputSchema := datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	partialSchema := datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "sum", Type: arrow.PrimitiveTypes.Float64},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
	}}
	resultSchema := datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "AVG", Type: arrow.PrimitiveTypes.Float64},
	}}
	scan := physicalplan.ScanExec{Source: testSource{schema: inputSchema}}
	plan := physicalplan.HashAggregateExec{
		Input:         scan,
		OutputSchema:  resultSchema,
		GroupExpr:     []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
		AggregateExpr: []physicalplan.AggregateExpr{physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 1}}},
	}

	got, err := BuildStages(plan)
	if err != nil {
		t.Fatalf("BuildStages() returned an error: %v", err)
	}

	want := []Stage{
		{
			ID: 0,
			Plan: ExchangeWriteExec{
				KeyIndex: 0,
				Input: physicalplan.HashAggregateExec{
					Input:         scan,
					Mode:          physicalplan.AggregatePartial,
					OutputSchema:  partialSchema,
					GroupExpr:     []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
					AggregateExpr: []physicalplan.AggregateExpr{physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 1}}},
				},
			},
		},
		{
			ID: 1, Dependencies: []int{0},
			Plan: physicalplan.HashAggregateExec{
				Input:        ExchangeReadExec{StageID: 0, OutputSchema: partialSchema},
				Mode:         physicalplan.AggregateFinal,
				OutputSchema: resultSchema,
				GroupExpr:    []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
				AggregateExpr: []physicalplan.AggregateExpr{physicalplan.AvgExpr{
					Expr:      physicalplan.ColumnExpr{Index: 1},
					CountExpr: physicalplan.ColumnExpr{Index: 2},
				}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildStages() = %#v; want %#v", got, want)
	}
}
