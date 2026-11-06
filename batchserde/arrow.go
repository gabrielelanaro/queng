// Package batchserde encodes query batches as Arrow IPC streams.
package batchserde

import (
	"bytes"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/gabrielelanaro/queng/datasource"
)

const qualifierKey = "queng.qualifier"

// Encode returns an Arrow IPC stream containing one batch. It borrows the batch.
func Encode(batch datasource.RecordBatch) ([]byte, error) {
	fields := make([]arrow.Field, batch.ColumnCount())
	columns := make([]arrow.Array, batch.ColumnCount())
	if len(batch.Schema.Fields) != batch.ColumnCount() {
		return nil, fmt.Errorf("batch schema does not match its columns")
	}
	for i, field := range batch.Schema.Fields {
		fields[i] = arrow.Field{Name: field.Name, Type: field.Type, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{qualifierKey: field.Qualifier})}
		switch col := batch.Column(i).(type) {
		case datasource.ArrowColumn:
			columns[i] = col.Array
		case *datasource.ArrowColumn:
			columns[i] = col.Array
		default:
			return nil, fmt.Errorf("unsupported column: %T", col)
		}
	}
	schema := arrow.NewSchema(fields, nil)
	rows := int64(0)
	if len(columns) > 0 {
		rows = int64(columns[0].Len())
	}
	record := array.NewRecord(schema, columns, rows)
	defer record.Release()
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode returns a batch owned by the caller, who must release it.
func Decode(data []byte) (datasource.RecordBatch, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return datasource.RecordBatch{}, err
	}
	defer reader.Release()
	record, err := reader.Read()
	if err != nil {
		return datasource.RecordBatch{}, err
	}
	var fields []datasource.Field
	var columns []datasource.ColumnVector
	for i, field := range record.Schema().Fields() {
		qualifier, _ := field.Metadata.GetValue(qualifierKey)
		fields = append(fields, datasource.Field{Name: field.Name, Qualifier: qualifier, Type: field.Type})
		col := record.Column(i)
		col.Retain()
		columns = append(columns, datasource.ArrowColumn{Array: col})
	}
	batch := datasource.NewRecordBatch(datasource.Schema{Fields: fields}, columns)
	if _, err := reader.Read(); err != io.EOF {
		batch.Release()
		if err == nil {
			return datasource.RecordBatch{}, fmt.Errorf("expected one record batch")
		}
		return datasource.RecordBatch{}, err
	}
	return batch, nil
}
