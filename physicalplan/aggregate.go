package physicalplan

import (
	"fmt"
	"iter"
	"reflect"
	"strings"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
)

// Accumulator maintains the state of an aggregate for one group.
type Accumulator interface {
	Accumulate(value any) error
	IntermediateValues() []any
	Merge(values []any) error
	FinalValue() any
}

// AvgAccumulator maintains the sum and count of non-null values.
type AvgAccumulator struct {
	sum   float64
	count int
}

func (a *AvgAccumulator) Accumulate(value any) error {
	var number float64
	switch v := value.(type) {
	case nil:
		return nil
	case int:
		number = float64(v)
	case int8:
		number = float64(v)
	case int16:
		number = float64(v)
	case int32:
		number = float64(v)
	case int64:
		number = float64(v)
	case float32:
		number = float64(v)
	case float64:
		number = v
	default:
		return fmt.Errorf("cannot average value of type %T", value)
	}
	a.sum += number
	a.count++
	return nil
}

func (a *AvgAccumulator) FinalValue() any {
	if a.count == 0 {
		return nil
	}
	return a.sum / float64(a.count)
}

func (a *AvgAccumulator) IntermediateValues() []any {
	return []any{a.sum, int64(a.count)}
}

func (a *AvgAccumulator) Merge(values []any) error {
	if len(values) != 2 {
		return fmt.Errorf("average state must contain sum and count")
	}
	sum, ok := values[0].(float64)
	if !ok {
		return fmt.Errorf("average sum must be float64, got %T", values[0])
	}
	count, ok := values[1].(int64)
	if !ok || count < 0 {
		return fmt.Errorf("average count must be a non-negative int64")
	}
	a.sum += sum
	a.count += int(count)
	return nil
}

type aggregateGroup struct {
	keys         []any
	accumulators []Accumulator
}

type groupNode struct {
	children map[any]*groupNode
	group    *aggregateGroup
}

type groupTable struct {
	root       groupNode
	groups     []*aggregateGroup
	aggregates []AggregateExpr
}

func newGroupTable(groupExprs []PhysicalExpr, aggregates []AggregateExpr) *groupTable {
	table := &groupTable{aggregates: aggregates}
	// An ungrouped aggregate produces one result even for empty input.
	if len(groupExprs) == 0 {
		table.root.group = table.newGroup(nil)
	}
	return table
}

func (t *groupTable) newGroup(keys []any) *aggregateGroup {
	group := &aggregateGroup{keys: keys}
	for _, expr := range t.aggregates {
		group.accumulators = append(group.accumulators, expr.NewAccumulator())
	}
	t.groups = append(t.groups, group)
	return group
}

func (t *groupTable) findOrCreateGroup(columns []datasource.ColumnVector, row int) (*aggregateGroup, error) {
	node := &t.root
	keys := make([]any, len(columns))
	for i, column := range columns {
		value := column.Value(row)
		if text, ok := value.(string); ok {
			value = strings.Clone(text)
		}
		keys[i] = value
		if value != nil && !reflect.ValueOf(value).Comparable() {
			return nil, fmt.Errorf("group value of type %T cannot be compared", value)
		}
		if node.children == nil {
			node.children = make(map[any]*groupNode)
		}
		if node.children[value] == nil {
			node.children[value] = &groupNode{}
		}
		node = node.children[value]
	}
	if node.group == nil {
		node.group = t.newGroup(keys)
	}
	return node.group, nil
}

// Execute consumes the input and returns one row per group.
func (a HashAggregateExec) Execute(ctx execution.Context, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		groups := newGroupTable(a.GroupExpr, a.AggregateExpr)
		for batch, err := range a.Input.Execute(ctx, partitionID) {
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
			err = a.processBatch(batch, groups)
			batch.Release()
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
		}
		result, err := a.buildResultBatch(groups)
		yield(result, err)
	}
}

func (a HashAggregateExec) processBatch(batch datasource.RecordBatch, groups *groupTable) error {
	groupColumns, err := evaluateExpressions(a.GroupExpr, batch)
	if err != nil {
		return err
	}
	aggregateColumns := make([][]datasource.ColumnVector, 0, len(a.AggregateExpr))
	for _, expr := range a.AggregateExpr {
		inputs := []PhysicalExpr{expr.InputExpression()}
		if a.Mode == AggregateFinal {
			avg, ok := expr.(AvgExpr)
			if !ok || avg.CountExpr == nil {
				return fmt.Errorf("final average requires sum and count expressions")
			}
			inputs = append(inputs, avg.CountExpr)
		}
		columns, err := evaluateExpressions(inputs, batch)
		if err != nil {
			return err
		}
		aggregateColumns = append(aggregateColumns, columns)
	}
	rows := 0
	if batch.ColumnCount() > 0 {
		rows = batch.Column(0).Len()
	}
	return processRows(rows, groupColumns, aggregateColumns, groups, a.Mode)
}

func evaluateExpressions(exprs []PhysicalExpr, batch datasource.RecordBatch) ([]datasource.ColumnVector, error) {
	columns := make([]datasource.ColumnVector, 0, len(exprs))
	for _, expr := range exprs {
		column, err := expr.Evaluate(batch)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func processRows(rows int, groupColumns []datasource.ColumnVector, aggregateColumns [][]datasource.ColumnVector, groups *groupTable, mode AggregateMode) error {
	for row := 0; row < rows; row++ {
		group, err := groups.findOrCreateGroup(groupColumns, row)
		if err != nil {
			return err
		}
		if err := accumulateRow(group, aggregateColumns, row, mode); err != nil {
			return err
		}
	}
	return nil
}

func accumulateRow(group *aggregateGroup, columns [][]datasource.ColumnVector, row int, mode AggregateMode) error {
	for i, accumulator := range group.accumulators {
		if mode == AggregateFinal {
			state := make([]any, len(columns[i]))
			for j, column := range columns[i] {
				state[j] = column.Value(row)
			}
			if err := accumulator.Merge(state); err != nil {
				return err
			}
		} else if err := accumulator.Accumulate(columns[i][0].Value(row)); err != nil {
			return err
		}
	}
	return nil
}

func (a HashAggregateExec) buildResultBatch(groups *groupTable) (datasource.RecordBatch, error) {
	values := make([][]any, len(a.OutputSchema.Fields))
	for _, group := range groups.groups {
		for i, key := range group.keys {
			values[i] = append(values[i], key)
		}
		index := len(a.GroupExpr)
		for _, accumulator := range group.accumulators {
			if a.Mode == AggregatePartial {
				for _, value := range accumulator.IntermediateValues() {
					values[index] = append(values[index], value)
					index++
				}
			} else {
				values[index] = append(values[index], accumulator.FinalValue())
				index++
			}
		}
	}
	var columns []datasource.ColumnVector
	for i, columnValues := range values {
		column, err := datasource.NewArrowColumn(a.OutputSchema.Fields[i].Type, columnValues)
		if err != nil {
			datasource.NewRecordBatch(a.OutputSchema, columns).Release()
			return datasource.RecordBatch{}, err
		}
		columns = append(columns, column)
	}
	return datasource.NewRecordBatch(a.OutputSchema, columns), nil
}
