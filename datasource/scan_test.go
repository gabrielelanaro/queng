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

func makeCSVInventory(t *testing.T) datasource.DataSource {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inventory.csv")
	if err := os.WriteFile(path, []byte("quantity,name,price\n2,book,10\n3,apple,20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewCSVSource(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func makeParquetInventory(t *testing.T) datasource.DataSource {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inventory.parquet")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	writer, err := pqarrow.NewFileWriter(schema, file, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(
		`[{"quantity":2,"name":"book","price":10},{"quantity":3,"name":"apple","price":20}]`,
	))
	if err != nil {
		writer.Close()
		t.Fatal(err)
	}
	err = writer.Write(record)
	record.Release()
	if err != nil {
		writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewParquetSource(path)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func TestScanReturnsOnlySelectedColumnsInRequestedOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source func(*testing.T) datasource.DataSource
	}{
		{"CSV", makeCSVInventory},
		{"Parquet", makeParquetInventory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.source(t)
			wantSchema := datasource.Schema{Fields: []datasource.Field{
				{Name: "price", Type: arrow.PrimitiveTypes.Int64},
				{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
			}}
			var rows [][]any
			for batch, err := range source.Scan(0, []int{2, 0}) {
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(batch.Schema, wantSchema) || batch.ColumnCount() != 2 {
					batch.Release()
					t.Fatalf("selected schema = %#v; want %#v", batch.Schema, wantSchema)
				}
				for row := range batch.Column(0).Len() {
					rows = append(rows, []any{batch.Column(0).Value(row), batch.Column(1).Value(row)})
				}
				batch.Release()
			}
			want := [][]any{{int64(10), int64(2)}, {int64(20), int64(3)}}
			if !reflect.DeepEqual(rows, want) {
				t.Errorf("selected rows = %v; want %v", rows, want)
			}
			if len(source.Schema().Fields) != 3 {
				t.Fatal("scan changed the source schema")
			}
		})
	}
}
