// parquet-fixture creates partitioned sales and product data for distributed queries.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

var categories = []string{"books", "electronics", "clothing", "groceries", "home", "garden", "sports", "toys"}

// Each record becomes one Parquet row group and therefore one scan task.
func write(path string, schema *arrow.Schema, rows, partitions int, appendRow func(*array.RecordBuilder, int)) error {
	file, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer os.Remove(path + ".tmp")
	writer, err := pqarrow.NewFileWriter(schema, file, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		file.Close()
		return err
	}
	for partition := range partitions {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		start, end := partition*rows/partitions, (partition+1)*rows/partitions
		builder.Reserve(end - start)
		for row := start; row < end; row++ {
			appendRow(builder, row)
		}
		record := builder.NewRecordBatch()
		builder.Release()
		err = writer.Write(record)
		record.Release()
		if err != nil {
			writer.Close()
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	fmt.Printf("%s: %d rows, %d partitions\n", path, rows, partitions)
	return nil
}

func main() {
	directory := flag.String("output", "samples/generated", "output directory")
	sales := flag.Int("sales", 1_000_000, "number of sales rows")
	products := flag.Int("products", 2_000, "number of products")
	salesPartitions := flag.Int("sales-partitions", 48, "sales row groups / scan tasks")
	productPartitions := flag.Int("product-partitions", 8, "product row groups / scan tasks")
	flag.Parse()
	if *sales < 1 || *products < 1 || *salesPartitions < 1 || *salesPartitions > *sales || *productPartitions < 1 || *productPartitions > *products {
		fmt.Fprintln(os.Stderr, "row counts must be positive; partition counts must be between 1 and their table's row count")
		os.Exit(1)
	}
	if err := os.MkdirAll(*directory, 0755); err != nil {
		panic(err)
	}
	productSchema := arrow.NewSchema([]arrow.Field{
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "product", Type: arrow.BinaryTypes.String},
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)
	if err := write(filepath.Join(*directory, "products.parquet"), productSchema, *products, *productPartitions, func(b *array.RecordBuilder, row int) {
		category := categories[row%len(categories)]
		b.Field(0).(*array.Int64Builder).Append(int64(row + 1))
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("%s-item-%04d", category, row+1))
		b.Field(2).(*array.StringBuilder).Append(category)
	}); err != nil {
		panic(err)
	}
	salesSchema := arrow.NewSchema([]arrow.Field{
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "sale", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	if err := write(filepath.Join(*directory, "sales.parquet"), salesSchema, *sales, *salesPartitions, func(b *array.RecordBuilder, row int) {
		// Popular products repeat more often; 2% of sales have no matching product.
		productID := (row*7919)%*products + 1
		if row%5 == 0 {
			productID = row%min(20, *products) + 1
		}
		if row%50 == 0 {
			productID = *products + 1
		}
		b.Field(0).(*array.Int64Builder).Append(int64(productID))
		b.Field(1).(*array.Int64Builder).Append(int64(5 + (row*37+productID*13)%496))
	}); err != nil {
		panic(err)
	}
}
