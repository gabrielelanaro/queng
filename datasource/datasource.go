// Package datasource defines the schema provided by a data source.
package datasource

import (
	"context"
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Field describes one column of a data source or query result.
type Field struct {
	Name      string
	Type      arrow.DataType
	Qualifier string
}

// Schema describes the ordered fields produced by a data source or plan.
type Schema struct {
	Fields []Field
}

// RecordBatch holds columns of values with their schema.
type RecordBatch struct {
	Schema  Schema
	columns []ColumnVector
}

// NewRecordBatch takes ownership of the column references and copies their slice.
func NewRecordBatch(schema Schema, columns []ColumnVector) RecordBatch {
	return RecordBatch{Schema: schema, columns: append([]ColumnVector(nil), columns...)}
}

// ColumnCount returns the number of columns in the batch.
func (b RecordBatch) ColumnCount() int { return len(b.columns) }

// Column returns a borrowed column, valid while the batch owns its reference.
func (b RecordBatch) Column(index int) ColumnVector { return b.columns[index] }

// Take returns a new batch containing the selected rows in the given order.
func (b RecordBatch) Take(rows []int) (RecordBatch, error) {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	for _, row := range rows {
		builder.Append(int64(row))
	}
	indices := builder.NewArray()
	defer indices.Release()

	var columns []ColumnVector
	for _, column := range b.columns {
		var values arrow.Array
		switch c := column.(type) {
		case ArrowColumn:
			values = c.Array
		case *ArrowColumn:
			values = c.Array
		default:
			NewRecordBatch(b.Schema, columns).Release()
			return RecordBatch{}, fmt.Errorf("cannot take rows from column type %T", column)
		}
		taken, err := compute.TakeArray(context.Background(), values, indices)
		if err != nil {
			NewRecordBatch(b.Schema, columns).Release()
			return RecordBatch{}, err
		}
		columns = append(columns, ArrowColumn{Array: taken})
	}
	return NewRecordBatch(b.Schema, columns), nil
}

// Retain acquires a reference to each column in the batch.
func (b RecordBatch) Retain() {
	for _, column := range b.columns {
		column.Retain()
	}
}

// Release releases the column references owned by the batch.
func (b RecordBatch) Release() {
	for _, column := range b.columns {
		column.Release()
	}
}

// DataSource provides the schema and batches of a table. Each batch yielded by
// Scan transfers its column references to the consumer, which must release it.
type DataSource interface {
	Schema() Schema
	PartitionCount() int
	Scan(partitionID int) iter.Seq2[RecordBatch, error]
}
