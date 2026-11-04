package physicalplan

import (
	"fmt"
	"iter"
	"reflect"
	"strings"

	"github.com/gabrielelanaro/queng/datasource"
)

// Accumulator maintains the state of an aggregate for one group.
type Accumulator interface {
	Accumulate(value any) error
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
func (a HashAggregateExec) Execute() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		groups := newGroupTable(a.GroupExpr, a.AggregateExpr)
		for batch, err := range a.Input.Execute() {
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
	inputs := make([]PhysicalExpr, 0, len(a.AggregateExpr))
	for _, expr := range a.AggregateExpr {
		inputs = append(inputs, expr.InputExpression())
	}
	aggregateColumns, err := evaluateExpressions(inputs, batch)
	if err != nil {
		return err
	}
	rows := 0
	if len(batch.Columns) > 0 {
		rows = batch.Columns[0].Len()
	}
	return processRows(rows, groupColumns, aggregateColumns, groups)
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

func processRows(rows int, groupColumns, aggregateColumns []datasource.ColumnVector, groups *groupTable) error {
	for row := 0; row < rows; row++ {
		group, err := groups.findOrCreateGroup(groupColumns, row)
		if err != nil {
			return err
		}
		if err := accumulateRow(group, aggregateColumns, row); err != nil {
			return err
		}
	}
	return nil
}

func accumulateRow(group *aggregateGroup, columns []datasource.ColumnVector, row int) error {
	for i, accumulator := range group.accumulators {
		if err := accumulator.Accumulate(columns[i].Value(row)); err != nil {
			return err
		}
	}
	return nil
}

func (a HashAggregateExec) buildResultBatch(groups *groupTable) (datasource.RecordBatch, error) {
	values := make([][]any, len(a.GroupExpr)+len(a.AggregateExpr))
	for _, group := range groups.groups {
		for i, key := range group.keys {
			values[i] = append(values[i], key)
		}
		for i, accumulator := range group.accumulators {
			index := len(a.GroupExpr) + i
			values[index] = append(values[index], accumulator.FinalValue())
		}
	}
	result := datasource.RecordBatch{Schema: a.Schema}
	for i, columnValues := range values {
		column, err := datasource.NewArrowColumn(a.Schema.Fields[i].Type, columnValues)
		if err != nil {
			result.Release()
			return datasource.RecordBatch{}, err
		}
		result.Columns = append(result.Columns, column)
	}
	return result, nil
}
