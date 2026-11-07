package datasource_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/gabrielelanaro/queng/datasource"
)

func TestParquetSourceReadsEachRowGroupAsPartition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "price", Type: arrow.PrimitiveTypes.Int64}}, nil)
	writer, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range []string{`[{"price":10},{"price":20}]`, `[{"price":30}]`} {
		record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(rows))
		if err != nil {
			t.Fatal(err)
		}
		err = writer.Write(record)
		record.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewParquetSource(path)
	if err != nil {
		t.Fatal(err)
	}
	if source.PartitionCount() != 2 {
		t.Fatalf("partition count = %d; want 2", source.PartitionCount())
	}
	var partitions [][]any
	for partition := range source.PartitionCount() {
		var values []any
		for batch, err := range source.Scan(partition) {
			if err != nil {
				t.Fatal(err)
			}
			for row := range batch.Column(0).Len() {
				values = append(values, batch.Column(0).Value(row))
			}
			batch.Release()
		}
		partitions = append(partitions, values)
	}
	want := [][]any{{int64(10), int64(20)}, {int64(30)}}
	if !reflect.DeepEqual(partitions, want) {
		t.Errorf("partitions = %v; want %v", partitions, want)
	}
}
