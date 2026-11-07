package executor

import (
	"context"
	"iter"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/physicalplan"
)

type executorTestSource struct {
	batch datasource.RecordBatch
}

func (s executorTestSource) Schema() datasource.Schema { return s.batch.Schema }

func (s executorTestSource) PartitionCount() int { return 1 }

func (s executorTestSource) Scan(partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		s.batch.Retain()
		yield(s.batch, nil)
	}
}

func makeInventorySource(t *testing.T) executorTestSource {
	t.Helper()
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	quantity, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, []any{int64(2), int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(quantity.Release)
	price, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Float64, []any{9.5, 12.0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(price.Release)
	return executorTestSource{batch: datasource.NewRecordBatch(schema, []datasource.ColumnVector{quantity, price})}
}

func TestLocalExecutorRunsStage(t *testing.T) {
	source := makeInventorySource(t)
	outputSchema := datasource.Schema{Fields: []datasource.Field{{Name: "price", Type: arrow.PrimitiveTypes.Float64}}}
	task := Task{
		JobID: "job-1",
		Stage: distributed.Stage{
			ID: 0,
			Plan: physicalplan.ProjectionExec{
				Input:        physicalplan.ScanExec{Source: source},
				OutputSchema: outputSchema,
				Expr:         []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 1}},
			},
		},
	}

	executor := LocalExecutor{}
	result := <-executor.Submit(context.Background(), task)
	if result.Err != nil {
		t.Fatalf("task returned an error: %v", result.Err)
	}
	for _, batch := range result.Batches {
		defer batch.Release()
	}
	if len(result.Batches) != 1 {
		t.Fatalf("got %d batches; want 1", len(result.Batches))
	}
	batch := result.Batches[0]
	if !reflect.DeepEqual(batch.Schema, outputSchema) {
		t.Fatalf("schema = %#v; want %#v", batch.Schema, outputSchema)
	}
	var values []any
	for i := 0; i < batch.Column(0).Len(); i++ {
		values = append(values, batch.Column(0).Value(i))
	}
	if !reflect.DeepEqual(values, []any{9.5, 12.0}) {
		t.Errorf("values = %#v; want [9.5 12]", values)
	}
}

func TestLocalExecutorReportsExecutionError(t *testing.T) {
	source := makeInventorySource(t)
	task := Task{JobID: "job-1", Stage: distributed.Stage{Plan: physicalplan.ProjectionExec{
		Input: physicalplan.ScanExec{Source: source},
		Expr:  []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 10}},
	}}}

	result := <-(LocalExecutor{}).Submit(context.Background(), task)
	if result.Err == nil {
		for _, batch := range result.Batches {
			batch.Release()
		}
		t.Fatal("task succeeded with an invalid column index")
	}
}

type partitionedSource struct{}

func (partitionedSource) Schema() datasource.Schema {
	return datasource.Schema{Fields: []datasource.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}}
}

func (partitionedSource) PartitionCount() int { return 2 }

func (s partitionedSource) Scan(partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		column, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, []any{int64(partitionID)})
		if err != nil {
			yield(datasource.RecordBatch{}, err)
			return
		}
		yield(datasource.NewRecordBatch(s.Schema(), []datasource.ColumnVector{column}), nil)
	}
}

func TestLocalExecutorExecutesAssignedPartition(t *testing.T) {
	source := partitionedSource{}
	task := Task{
		PartitionID: 1,
		Stage: distributed.Stage{Plan: physicalplan.ProjectionExec{
			Input: physicalplan.ScanExec{Source: source}, OutputSchema: source.Schema(),
			Expr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
		}},
	}
	result := <-(LocalExecutor{}).Submit(context.Background(), task)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	for _, batch := range result.Batches {
		defer batch.Release()
	}
	if len(result.Batches) != 1 || result.Batches[0].Column(0).Value(0) != int64(1) {
		t.Fatalf("unexpected result for partition 1: %#v", result.Batches)
	}
}
