package physicalplan

import (
	"fmt"
	"iter"
	"reflect"
	"strings"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
)

func joinKey(batch datasource.RecordBatch, row, index int) (any, error) {
	if index < 0 || index >= len(batch.Columns) {
		return nil, fmt.Errorf("join column index %d out of range", index)
	}
	value := batch.Columns[index].Value(row)
	if value != nil && !reflect.ValueOf(value).Comparable() {
		return nil, fmt.Errorf("join value of type %T cannot be compared", value)
	}
	return value, nil
}

func (j HashJoinExec) Execute() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		if j.Type != logicalplan.LeftJoin || len(j.LeftKeys) != 1 || len(j.RightKeys) != 1 {
			yield(datasource.RecordBatch{}, fmt.Errorf("hash join requires a left join with one key per input"))
			return
		}
		table, err := j.buildHashTable()
		if err != nil {
			yield(datasource.RecordBatch{}, err)
			return
		}
		for batch, err := range j.Left.Execute() {
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

func (j HashJoinExec) buildHashTable() (map[any][][]any, error) {
	table := make(map[any][][]any)
	for batch, err := range j.Right.Execute() {
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
	if len(batch.Columns) == 0 {
		return nil
	}
	for row := 0; row < batch.Columns[0].Len(); row++ {
		key, err := joinKey(batch, row, keyIndex)
		if err != nil {
			return err
		}
		if key != nil {
			values := make([]any, len(batch.Columns))
			for i, column := range batch.Columns {
				values[i] = copyJoinValue(column.Value(row))
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
	values := make([][]any, len(j.Schema.Fields))
	if len(batch.Columns) > len(values) {
		return datasource.RecordBatch{}, fmt.Errorf("join output schema has too few columns")
	}
	if len(batch.Columns) > 0 {
		for row := 0; row < batch.Columns[0].Len(); row++ {
			key, err := joinKey(batch, row, j.LeftKeys[0])
			if err != nil {
				return datasource.RecordBatch{}, err
			}
			matches := table[key]
			if len(matches) == 0 {
				appendJoinedRow(values, batch, row, nil)
			} else {
				for _, right := range matches {
					if len(batch.Columns)+len(right) != len(values) {
						return datasource.RecordBatch{}, fmt.Errorf("join output schema does not match inputs")
					}
					appendJoinedRow(values, batch, row, right)
				}
			}
		}
	}
	result := datasource.RecordBatch{Schema: j.Schema}
	for i, columnValues := range values {
		column, err := datasource.NewArrowColumn(j.Schema.Fields[i].Type, columnValues)
		if err != nil {
			result.Release()
			return datasource.RecordBatch{}, err
		}
		result.Columns = append(result.Columns, column)
	}
	return result, nil
}

func appendJoinedRow(values [][]any, left datasource.RecordBatch, row int, right []any) {
	for i, column := range left.Columns {
		values[i] = append(values[i], column.Value(row))
	}
	for i := len(left.Columns); i < len(values); i++ {
		var value any
		if right != nil {
			value = right[i-len(left.Columns)]
		}
		values[i] = append(values[i], value)
	}
}
