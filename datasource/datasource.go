// Package datasource defines the schema provided by a data source.
package datasource

import (
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
)

// Field describes one column of a data source or query result.
type Field struct {
	Name string
	Type arrow.DataType
}

// Schema describes the ordered fields produced by a data source or plan.
type Schema struct {
	Fields []Field
}

// RecordBatch holds columns of values with their schema.
type RecordBatch struct {
	Schema  Schema
	Columns [][]any
}

// DataSource provides the schema and batches of a table.
type DataSource interface {
	Schema() Schema
	Scan() iter.Seq2[RecordBatch, error]
}
