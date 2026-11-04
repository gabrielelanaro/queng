package datasource

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ColumnVector is a column of values in a record batch.
// Retain and Release manage a reference to its underlying storage.
type ColumnVector interface {
	DataType() arrow.DataType
	Len() int
	Value(i int) any
	Retain()
	Release()
}

// ArrowColumn wraps an Arrow array. It takes ownership of one array reference.
type ArrowColumn struct {
	Array arrow.Array
}

func (c ArrowColumn) DataType() arrow.DataType { return c.Array.DataType() }
func (c ArrowColumn) Len() int                 { return c.Array.Len() }
func (c ArrowColumn) Retain()                  { c.Array.Retain() }
func (c ArrowColumn) Release()                 { c.Array.Release() }

func (c ArrowColumn) Value(i int) any {
	if c.Array.IsNull(i) {
		return nil
	}
	return c.Array.GetOneForMarshal(i)
}

// NewArrowColumn builds a column for the supported field types.
func NewArrowColumn(dataType arrow.DataType, values []any) (ColumnVector, error) {
	if dataType == nil {
		return nil, fmt.Errorf("missing Arrow type")
	}
	switch dataType.ID() {
	case arrow.INT64, arrow.FLOAT64, arrow.STRING, arrow.BOOL:
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", dataType)
	}
	builder := array.NewBuilder(memory.DefaultAllocator, dataType)
	defer builder.Release()
	for _, value := range values {
		if value == nil {
			builder.AppendNull()
			continue
		}
		switch b := builder.(type) {
		case *array.Int64Builder:
			v, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("expected int64, got %T", value)
			}
			b.Append(v)
		case *array.Float64Builder:
			v, ok := value.(float64)
			if !ok {
				return nil, fmt.Errorf("expected float64, got %T", value)
			}
			b.Append(v)
		case *array.StringBuilder:
			v, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected string, got %T", value)
			}
			b.Append(v)
		case *array.BooleanBuilder:
			v, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("expected bool, got %T", value)
			}
			b.Append(v)
		default:
			return nil, fmt.Errorf("unsupported Arrow type: %s", dataType)
		}
	}
	return ArrowColumn{Array: builder.NewArray()}, nil
}
