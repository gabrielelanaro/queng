package physicalplan_test

import (
	"context"
	"iter"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/physicalplan"
)

// batchInput supplies fixed batches directly to the operator under test.
type batchInput struct{ batchesSource }

func (batchInput) Children() []physicalplan.PhysicalPlan { return nil }
func (s batchInput) Execute(_ context.Context, _ execution.Environment, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return s.Scan(partitionID, nil)
}

func averageStateSchema() datasource.Schema {
	return datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "sum", Type: arrow.PrimitiveTypes.Float64},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
	}}
}

func TestPartialAverageEmitsSumAndCountPerGroup(t *testing.T) {
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	input := batchInput{batchesSource{schema: schema, batches: []datasource.RecordBatch{
		makeBatch(t, schema, []any{"books", "fruit"}, []any{10.0, 8.0}),
		makeBatch(t, schema, []any{"books", "books"}, []any{20.0, nil}),
	}}}
	plan := physicalplan.HashAggregateExec{
		Input:        input,
		Mode:         physicalplan.AggregatePartial,
		OutputSchema: averageStateSchema(),
		GroupExpr:    []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
		AggregateExpr: []physicalplan.AggregateExpr{
			physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 1}},
		},
	}

	assertAggregateRows(t, plan, map[string][]any{
		"books": {30.0, int64(2)},
		"fruit": {8.0, int64(1)},
	})
}

func TestFinalAverageMergesSumsAndCountsPerGroup(t *testing.T) {
	schema := averageStateSchema()
	input := batchInput{batchesSource{schema: schema, batches: []datasource.RecordBatch{
		makeBatch(t, schema, []any{"books"}, []any{30.0}, []any{int64(2)}),
		makeBatch(t, schema, []any{"books", "fruit"}, []any{30.0, 8.0}, []any{int64(1), int64(1)}),
	}}}
	plan := physicalplan.HashAggregateExec{
		Input: input,
		Mode:  physicalplan.AggregateFinal,
		OutputSchema: datasource.Schema{Fields: []datasource.Field{
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "AVG", Type: arrow.PrimitiveTypes.Float64},
		}},
		GroupExpr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
		AggregateExpr: []physicalplan.AggregateExpr{
			physicalplan.AvgExpr{
				Expr:      physicalplan.ColumnExpr{Index: 1},
				CountExpr: physicalplan.ColumnExpr{Index: 2},
			},
		},
	}

	// Unequal counts distinguish a merged average from an average of averages.
	assertAggregateRows(t, plan, map[string][]any{
		"books": {20.0},
		"fruit": {8.0},
	})
}

func assertAggregateRows(t *testing.T, plan physicalplan.HashAggregateExec, want map[string][]any) {
	t.Helper()
	got := make(map[string][]any)
	for batch, err := range plan.Execute(context.Background(), execution.Environment{}, 0) {
		if err != nil {
			t.Fatal(err)
		}
		for row := range batch.Column(0).Len() {
			key := strings.Clone(batch.Column(0).Value(row).(string))
			values := make([]any, batch.ColumnCount()-1)
			for i := range values {
				values[i] = batch.Column(i + 1).Value(row)
			}
			got[key] = values
		}
		batch.Release()
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("aggregate rows = %v; want %v", got, want)
	}
}

func TestUngroupedFinalAverageReturnsOneRow(t *testing.T) {
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "sum", Type: arrow.PrimitiveTypes.Float64},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
	}}
	for _, tc := range []struct {
		name   string
		sums   []any
		counts []any
		want   any
	}{
		{name: "unequal partial counts", sums: []any{30.0, 30.0}, counts: []any{int64(2), int64(1)}, want: 20.0},
		{name: "empty partials", sums: []any{0.0, 0.0}, counts: []any{int64(0), int64(0)}, want: nil},
		{name: "no rows", want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := batchInput{batchesSource{schema: schema, batches: []datasource.RecordBatch{
				makeBatch(t, schema, tc.sums, tc.counts),
			}}}
			plan := physicalplan.HashAggregateExec{
				Input: input,
				Mode:  physicalplan.AggregateFinal,
				OutputSchema: datasource.Schema{Fields: []datasource.Field{
					{Name: "AVG", Type: arrow.PrimitiveTypes.Float64},
				}},
				AggregateExpr: []physicalplan.AggregateExpr{
					physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 0}, CountExpr: physicalplan.ColumnExpr{Index: 1}},
				},
			}

			var got []any
			for batch, err := range plan.Execute(context.Background(), execution.Environment{}, 0) {
				if err != nil {
					t.Fatal(err)
				}
				for row := range batch.Column(0).Len() {
					got = append(got, batch.Column(0).Value(row))
				}
				batch.Release()
			}
			if !reflect.DeepEqual(got, []any{tc.want}) {
				t.Errorf("average rows = %v; want [%v]", got, tc.want)
			}
		})
	}
}
