package datasource

import (
	"context"
	"fmt"
	"iter"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ParquetSource reads one file, with one partition per row group.
type ParquetSource struct {
	path       string
	schema     Schema
	partitions int
}

func NewParquetSource(path string) (*ParquetSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	arrowSchema, err := arrowReader.Schema()
	if err != nil {
		return nil, err
	}
	schema := Schema{}
	for _, field := range arrowSchema.Fields() {
		switch field.Type.ID() {
		case arrow.INT64, arrow.FLOAT64, arrow.STRING, arrow.BOOL:
		default:
			return nil, fmt.Errorf("unsupported Parquet column %q type %s", field.Name, field.Type)
		}
		schema.Fields = append(schema.Fields, Field{Name: field.Name, Type: field.Type})
	}
	return &ParquetSource{path: path, schema: schema, partitions: reader.NumRowGroups()}, nil
}

func (s *ParquetSource) Path() string        { return s.path }
func (s *ParquetSource) Schema() Schema      { return s.schema }
func (s *ParquetSource) PartitionCount() int { return s.partitions }

// Scan yields batches from one row group. The caller owns each batch.
func (s *ParquetSource) Scan(partitionID int) iter.Seq2[RecordBatch, error] {
	return func(yield func(RecordBatch, error) bool) {
		if partitionID < 0 || partitionID >= s.partitions {
			yield(RecordBatch{}, fmt.Errorf("Parquet partition %d out of range", partitionID))
			return
		}
		f, err := os.Open(s.path)
		if err != nil {
			yield(RecordBatch{}, err)
			return
		}
		defer f.Close()
		reader, err := file.NewParquetReader(f)
		if err != nil {
			yield(RecordBatch{}, err)
			return
		}
		defer reader.Close()
		arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
		if err != nil {
			yield(RecordBatch{}, err)
			return
		}
		records, err := arrowReader.GetRecordReader(context.Background(), nil, []int{partitionID})
		if err != nil {
			yield(RecordBatch{}, err)
			return
		}
		defer records.Release()
		for records.Next() {
			record := records.RecordBatch()
			columns := make([]ColumnVector, record.NumCols())
			for i := range columns {
				column := record.Column(i)
				column.Retain()
				columns[i] = ArrowColumn{Array: column}
			}
			if !yield(NewRecordBatch(s.schema, columns), nil) {
				return
			}
		}
		if err := records.Err(); err != nil {
			yield(RecordBatch{}, err)
		}
	}
}
