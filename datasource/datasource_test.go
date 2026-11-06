package datasource_test

import (
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
