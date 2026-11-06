package datasource_test

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/gabrielelanaro/queng/datasource"
)

func TestRecordBatchKeepsItsColumnList(t *testing.T) {
	first, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, []any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, []any{int64(2)})
	if err != nil {
		first.Release()
		t.Fatal(err)
	}
	columns := []datasource.ColumnVector{first, second}
	batch := datasource.NewRecordBatch(datasource.Schema{}, columns)
	defer batch.Release()

	columns[0], columns[1] = columns[1], columns[0]
	if got := batch.Column(0).Value(0); got != int64(1) {
		t.Errorf("first column value = %v; want 1", got)
	}
}

func TestRecordBatchTakeSelectsRows(t *testing.T) {
	ids, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Int64, []any{int64(1), int64(2), int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	prices, err := datasource.NewArrowColumn(arrow.PrimitiveTypes.Float64, []any{10.0, 20.0, nil})
	if err != nil {
		ids.Release()
		t.Fatal(err)
	}
	schema := datasource.Schema{Fields: []datasource.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
	}}
	batch := datasource.NewRecordBatch(schema, []datasource.ColumnVector{ids, prices})
	selected, err := batch.Take([]int{2, 0})
	batch.Release()
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Release()
	if !reflect.DeepEqual(selected.Schema, schema) {
		t.Fatalf("schema = %#v; want %#v", selected.Schema, schema)
	}
	got := [][]any{
		{selected.Column(0).Value(0), selected.Column(1).Value(0)},
		{selected.Column(0).Value(1), selected.Column(1).Value(1)},
	}
	want := [][]any{{int64(3), nil}, {int64(1), 10.0}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("selected rows = %#v; want %#v", got, want)
	}
}
