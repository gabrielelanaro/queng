package datasource

import (
	"path/filepath"
	"strings"
)

// NewFileSource opens a Parquet file or a CSV file based on its extension.
func NewFileSource(path string, batchSize int) (DataSource, error) {
	if strings.EqualFold(filepath.Ext(path), ".parquet") {
		return NewParquetSource(path)
	}
	return NewCSVSource(path, batchSize)
}
