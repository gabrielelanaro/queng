package physicalplan

import (
	"fmt"
	"iter"
	"reflect"
	"strings"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/logicalplan"
)

func joinKey(batch datasource.RecordBatch, row, index int) (any, error) {
	if index < 0 || index >= batch.ColumnCount() {
		return nil, fmt.Errorf("join column index %d out of range", index)
	}
	value := batch.Column(index).Value(row)
	if value != nil && !reflect.ValueOf(value).Comparable() {
		return nil, fmt.Errorf("join value of type %T cannot be compared", value)
	}
	return value, nil
}

func (j HashJoinExec) Execute(ctx execution.Context, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		if j.Type != logicalplan.LeftJoin || len(j.LeftKeys) != 1 || len(j.RightKeys) != 1 {
			yield(datasource.RecordBatch{}, fmt.Errorf("hash join requires a left join with one key per input"))
			return
		}
		table, err := j.buildHashTable(ctx, partitionID)
		if err != nil {
			yield(datasource.RecordBatch{}, err)
			return
		}
		for batch, err := range j.Left.Execute(ctx, partitionID) {
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
			result, err := j.joinBatch(table, batch)
			batch.Release()
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
			if !yield(result, nil) {
				return
			}
		}
	}
}

func (j HashJoinExec) buildHashTable(ctx execution.Context, partitionID int) (map[any][][]any, error) {
	table := make(map[any][][]any)
	for batch, err := range j.Right.Execute(ctx, partitionID) {
		if err != nil {
			return nil, err
		}
		err = addBatch(table, batch, j.RightKeys[0])
		batch.Release()
		if err != nil {
			return nil, err
		}
	}
	return table, nil
}

func addBatch(table map[any][][]any, batch datasource.RecordBatch, keyIndex int) error {
	if batch.ColumnCount() == 0 {
		return nil
	}
	for row := 0; row < batch.Column(0).Len(); row++ {
		key, err := joinKey(batch, row, keyIndex)
		if err != nil {
			return err
		}
		if key != nil {
			values := make([]any, batch.ColumnCount())
			for i := 0; i < batch.ColumnCount(); i++ {
				values[i] = copyJoinValue(batch.Column(i).Value(row))
			}
			key = copyJoinValue(key)
			table[key] = append(table[key], values)
		}
	}
	return nil
}

// Arrow strings may refer to batch buffers, which are released after the build.
func copyJoinValue(value any) any {
	if s, ok := value.(string); ok {
		return strings.Clone(s)
	}
	return value
}

func (j HashJoinExec) joinBatch(table map[any][][]any, batch datasource.RecordBatch) (datasource.RecordBatch, error) {
	values := make([][]any, len(j.OutputSchema.Fields))
	if batch.ColumnCount() > len(values) {
		return datasource.RecordBatch{}, fmt.Errorf("join output schema has too few columns")
	}
	if batch.ColumnCount() > 0 {
		for row := 0; row < batch.Column(0).Len(); row++ {
			key, err := joinKey(batch, row, j.LeftKeys[0])
			if err != nil {
				return datasource.RecordBatch{}, err
			}
			matches := table[key]
			if len(matches) == 0 {
				appendJoinedRow(values, batch, row, nil)
			} else {
				for _, right := range matches {
					if batch.ColumnCount()+len(right) != len(values) {
						return datasource.RecordBatch{}, fmt.Errorf("join output schema does not match inputs")
					}
					appendJoinedRow(values, batch, row, right)
				}
			}
		}
	}
	var columns []datasource.ColumnVector
	for i, columnValues := range values {
		column, err := datasource.NewArrowColumn(j.OutputSchema.Fields[i].Type, columnValues)
		if err != nil {
			datasource.NewRecordBatch(j.OutputSchema, columns).Release()
			return datasource.RecordBatch{}, err
		}
		columns = append(columns, column)
	}
	return datasource.NewRecordBatch(j.OutputSchema, columns), nil
}

func appendJoinedRow(values [][]any, left datasource.RecordBatch, row int, right []any) {
	for i := 0; i < left.ColumnCount(); i++ {
		values[i] = append(values[i], left.Column(i).Value(row))
	}
	for i := left.ColumnCount(); i < len(values); i++ {
		var value any
		if right != nil {
			value = right[i-left.ColumnCount()]
		}
		values[i] = append(values[i], value)
	}
}
