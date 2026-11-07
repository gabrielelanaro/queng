package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestLocalQueryReadsAllParquetPartitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.parquet")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "price", Type: arrow.PrimitiveTypes.Int64}}, nil)
	writer, err := pqarrow.NewFileWriter(schema, file, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`[{"price":10}]`, `[{"price":20}]`} {
		record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
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
	var output, errors bytes.Buffer
	if err := run([]string{"SELECT price FROM inventory", "--table", "inventory=" + path}, &output, &errors); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "10") || !strings.Contains(output.String(), "20") {
		t.Errorf("output = %s; want both rows", output.String())
	}
	if err := run([]string{"SELECT AVG(price) FROM inventory", "--table", "inventory=" + path}, &output, &errors); err == nil {
		t.Fatal("multi-partition local aggregation succeeded; want an error")
	}
}
