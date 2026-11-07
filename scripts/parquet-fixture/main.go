// parquet-fixture creates small join inputs with three row groups each.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func write(path string, fields []arrow.Field, groups []string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	schema := arrow.NewSchema(fields, nil)
	writer, err := pqarrow.NewFileWriter(schema, file, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		file.Close()
		return err
	}
	for _, data := range groups {
		record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
		if err != nil {
			writer.Close()
			return err
		}
		err = writer.Write(record)
		record.Release()
		if err != nil {
			writer.Close()
			return err
		}
	}
	return writer.Close()
}
func main() {
	directory := "samples/generated"
	if err := os.MkdirAll(directory, 0755); err != nil {
		panic(err)
	}
	salesFields := []arrow.Field{{Name: "product_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "sale", Type: arrow.PrimitiveTypes.Int64}}
	productFields := []arrow.Field{{Name: "product_id", Type: arrow.PrimitiveTypes.Int64}, {Name: "product", Type: arrow.BinaryTypes.String}}
	if err := write(filepath.Join(directory, "sales.parquet"), salesFields, []string{`[{"product_id":1,"sale":10}]`, `[{"product_id":2,"sale":20}]`, `[{"product_id":3,"sale":30}]`}); err != nil {
		panic(err)
	}
	if err := write(filepath.Join(directory, "products.parquet"), productFields, []string{`[{"product_id":1,"product":"apple"}]`, `[{"product_id":1,"product":"pear"}]`, `[{"product_id":2,"product":"book"}]`}); err != nil {
		panic(err)
	}
	fmt.Println("Created sales.parquet and products.parquet (three row groups each)")
}
