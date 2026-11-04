// Package datasource defines the schema provided by a data source.
package datasource

// Field describes one column of a data source or query result.
type Field struct {
	Name string
	Type string
}

// Schema describes the ordered fields produced by a data source or plan.
type Schema struct {
	Fields []Field
}

// DataSource provides the schema of a table.
type DataSource interface {
	Schema() Schema
}
