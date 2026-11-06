package datasource

import (
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// CSVSource reads a CSV file with a header, inferring types from its values.
type CSVSource struct {
	path      string
	schema    Schema
	batchSize int
}

// NewCSVSource reads the file to infer its schema. Empty fields are nulls.
func NewCSVSource(path string, batchSize int) (*CSVSource, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	seen := make(map[string]bool)
	schema := Schema{Fields: make([]Field, len(header))}
	for i, name := range header {
		if name == "" || seen[name] {
			return nil, fmt.Errorf("CSV header contains an empty or duplicate name: %q", name)
		}
		seen[name] = true
		schema.Fields[i].Name = name
	}
	// Each column starts without a type, then widens as values are read.
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV: %w", err)
		}
		for i, value := range record {
			if value == "" {
				continue
			}
			field := &schema.Fields[i]
			field.Type = inferCSVType(field.Type, value)
		}
	}
	for i := range schema.Fields {
		if schema.Fields[i].Type == nil {
			schema.Fields[i].Type = arrow.BinaryTypes.String
		}
	}
	return &CSVSource{path: path, schema: schema, batchSize: batchSize}, nil
}

func inferCSVType(current arrow.DataType, value string) arrow.DataType {
	if current != nil && current.ID() == arrow.STRING {
		return current
	}
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		if current != nil && current.ID() == arrow.FLOAT64 {
			return current
		}
		return arrow.PrimitiveTypes.Int64
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return arrow.PrimitiveTypes.Float64
	}
	return arrow.BinaryTypes.String
}

func (s *CSVSource) Schema() Schema { return s.schema }

// PartitionCount returns one partition for the entire CSV file.
func (s *CSVSource) PartitionCount() int { return 1 }

// Scan reopens the file and yields batches owned by the consumer.
func (s *CSVSource) Scan(partitionID int) iter.Seq2[RecordBatch, error] {
	return func(yield func(RecordBatch, error) bool) {
		if partitionID != 0 {
			yield(RecordBatch{}, fmt.Errorf("CSV partition %d out of range", partitionID))
			return
		}
		file, err := os.Open(s.path)
		if err != nil {
			yield(RecordBatch{}, err)
			return
		}
		defer file.Close()
		reader := csv.NewReader(file)
		header, err := reader.Read()
		if err != nil {
			yield(RecordBatch{}, fmt.Errorf("read CSV header: %w", err))
			return
		}
		if len(header) != len(s.schema.Fields) {
			yield(RecordBatch{}, fmt.Errorf("CSV header changed since schema inference"))
			return
		}
		for i, field := range s.schema.Fields {
			if header[i] != field.Name {
				yield(RecordBatch{}, fmt.Errorf("CSV header changed since schema inference"))
				return
			}
		}
		values := make([][]any, len(header))
		rowCount := 0
		for {
			record, readErr := reader.Read()
			if readErr != nil && readErr != io.EOF {
				yield(RecordBatch{}, fmt.Errorf("read CSV: %w", readErr))
				return
			}
			if readErr == nil {
				for i, value := range record {
					parsed, err := parseCSVValue(value, s.schema.Fields[i].Type)
					if err != nil {
						yield(RecordBatch{}, fmt.Errorf("column %q: %w", s.schema.Fields[i].Name, err))
						return
					}
					values[i] = append(values[i], parsed)
				}
				rowCount++
			}
			if rowCount == s.batchSize || (readErr == io.EOF && rowCount > 0) {
				var columns []ColumnVector
				for i, field := range s.schema.Fields {
					column, err := NewArrowColumn(field.Type, values[i])
					if err != nil {
						NewRecordBatch(s.schema, columns).Release()
						yield(RecordBatch{}, err)
						return
					}
					columns = append(columns, column)
				}
				batch := NewRecordBatch(s.schema, columns)
				if !yield(batch, nil) {
					return
				}
				values = make([][]any, len(header))
				rowCount = 0
			}
			if readErr == io.EOF {
				return
			}
		}
	}
}

func parseCSVValue(value string, dataType arrow.DataType) (any, error) {
	if value == "" {
		return nil, nil
	}
	switch dataType.ID() {
	case arrow.INT64:
		return strconv.ParseInt(value, 10, 64)
	case arrow.FLOAT64:
		return strconv.ParseFloat(value, 64)
	case arrow.STRING:
		return strings.Clone(value), nil
	default:
		return nil, fmt.Errorf("unsupported CSV type %s", dataType)
	}
}
