// Package physicalplan builds physical operations from logical plans.
package physicalplan

import (
	"fmt"
	"iter"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
)

// PhysicalPlan is an operation in a physical query plan.
type PhysicalPlan interface {
	Children() []PhysicalPlan
	Execute() iter.Seq2[datasource.RecordBatch, error]
}

// PhysicalExpr is an expression in a physical query plan. Evaluate returns a
// borrowed column valid for as long as the input batch remains alive.
type PhysicalExpr interface {
	String() string
	Evaluate(batch datasource.RecordBatch) (datasource.ColumnVector, error)
}

// AggregateExpr describes an aggregate operation on an input expression.
type AggregateExpr interface {
	InputExpression() PhysicalExpr
	NewAccumulator() Accumulator
}

// AvgExpr describes an average over an input expression.
type AvgExpr struct {
	Expr PhysicalExpr
}

func (a AvgExpr) InputExpression() PhysicalExpr { return a.Expr }

func (a AvgExpr) NewAccumulator() Accumulator { return &AvgAccumulator{} }

// ScanExec represents a scan of a data source.
type ScanExec struct {
	Source datasource.DataSource
}

func (ScanExec) Children() []PhysicalPlan { return nil }

func (s ScanExec) Execute() iter.Seq2[datasource.RecordBatch, error] {
	return s.Source.Scan()
}

// ProjectionExec represents a projection over an input plan.
type ProjectionExec struct {
	Input  PhysicalPlan
	Schema datasource.Schema
	Expr   []PhysicalExpr
}

func (p ProjectionExec) Children() []PhysicalPlan { return []PhysicalPlan{p.Input} }

func (p ProjectionExec) Execute() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		for batch, err := range p.Input.Execute() {
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
			result := datasource.RecordBatch{Schema: p.Schema}
			for _, expr := range p.Expr {
				column, err := expr.Evaluate(batch)
				if err != nil {
					result.Release()
					batch.Release()
					yield(datasource.RecordBatch{}, err)
					return
				}
				column.Retain()
				result.Columns = append(result.Columns, column)
			}
			batch.Release()
			if !yield(result, nil) {
				return
			}
		}
	}
}

// HashAggregateExec groups rows and computes aggregate expressions.
type HashAggregateExec struct {
	Input         PhysicalPlan
	Schema        datasource.Schema
	GroupExpr     []PhysicalExpr
	AggregateExpr []AggregateExpr
}

func (a HashAggregateExec) Children() []PhysicalPlan { return []PhysicalPlan{a.Input} }

// HashJoinExec joins two inputs using a hash table.
type HashJoinExec struct {
	Left      PhysicalPlan
	Right     PhysicalPlan
	Type      logicalplan.JoinType
	LeftKeys  []int
	RightKeys []int
	Schema    datasource.Schema
}

func (j HashJoinExec) Children() []PhysicalPlan { return []PhysicalPlan{j.Left, j.Right} }

// ColumnExpr refers to a column by its position in the input schema.
type ColumnExpr struct {
	Index int
}

func (c ColumnExpr) String() string { return fmt.Sprintf("column[%d]", c.Index) }

func (c ColumnExpr) Evaluate(batch datasource.RecordBatch) (datasource.ColumnVector, error) {
	if c.Index < 0 || c.Index >= len(batch.Columns) {
		return nil, fmt.Errorf("column index %d out of range", c.Index)
	}
	return batch.Columns[c.Index], nil
}

// BuildPhysicalPlan converts a logical plan to a physical plan.
func BuildPhysicalPlan(plan logicalplan.LogicalPlan) (PhysicalPlan, error) {
	switch p := plan.(type) {
	case logicalplan.Scan:
		return ScanExec{Source: p.Source}, nil
	case *logicalplan.Scan:
		if p == nil {
			return nil, fmt.Errorf("nil logical scan")
		}
		return BuildPhysicalPlan(*p)
	case logicalplan.Projection:
		return buildProjection(p)
	case *logicalplan.Projection:
		if p == nil {
			return nil, fmt.Errorf("nil logical projection")
		}
		return buildProjection(*p)
	case logicalplan.Aggregate:
		return buildAggregate(p)
	case *logicalplan.Aggregate:
		if p == nil {
			return nil, fmt.Errorf("nil logical aggregate")
		}
		return buildAggregate(*p)
	case logicalplan.Join:
		return buildJoin(p)
	case *logicalplan.Join:
		if p == nil {
			return nil, fmt.Errorf("nil logical join")
		}
		return buildJoin(*p)
	default:
		return nil, fmt.Errorf("unsupported logical plan: %T", plan)
	}
}

func buildJoin(j logicalplan.Join) (PhysicalPlan, error) {
	if j.Type != logicalplan.LeftJoin {
		return nil, fmt.Errorf("unsupported join type: %v", j.Type)
	}
	left, err := BuildPhysicalPlan(j.Left)
	if err != nil {
		return nil, err
	}
	right, err := BuildPhysicalPlan(j.Right)
	if err != nil {
		return nil, err
	}
	leftSchema, err := j.Left.Schema()
	if err != nil {
		return nil, err
	}
	rightSchema, err := j.Right.Schema()
	if err != nil {
		return nil, err
	}
	schema, err := j.Schema()
	if err != nil {
		return nil, err
	}
	var leftKeys, rightKeys []int
	for _, key := range j.On {
		leftIndex, err := findJoinKey(key.Left, leftSchema)
		if err != nil {
			return nil, err
		}
		rightIndex, err := findJoinKey(key.Right, rightSchema)
		if err != nil {
			return nil, err
		}
		leftKeys = append(leftKeys, leftIndex)
		rightKeys = append(rightKeys, rightIndex)
	}
	return HashJoinExec{
		Left: left, Right: right, Type: j.Type,
		LeftKeys: leftKeys, RightKeys: rightKeys, Schema: schema,
	}, nil
}

func findJoinKey(name string, schema datasource.Schema) (int, error) {
	index := -1
	for i, field := range schema.Fields {
		if field.Name == name {
			if index >= 0 {
				return 0, fmt.Errorf("ambiguous join column %q", name)
			}
			index = i
		}
	}
	if index < 0 {
		return 0, fmt.Errorf("no join column named %q", name)
	}
	return index, nil
}

func buildAggregate(a logicalplan.Aggregate) (PhysicalPlan, error) {
	input, err := BuildPhysicalPlan(a.Input)
	if err != nil {
		return nil, err
	}
	inputSchema, err := a.Input.Schema()
	if err != nil {
		return nil, err
	}
	outputSchema, err := a.Schema()
	if err != nil {
		return nil, err
	}
	groupExprs := make([]PhysicalExpr, 0, len(a.GroupExpr))
	for _, expr := range a.GroupExpr {
		physicalExpr, err := buildPhysicalExpr(expr, inputSchema)
		if err != nil {
			return nil, err
		}
		groupExprs = append(groupExprs, physicalExpr)
	}
	aggregates := make([]AggregateExpr, 0, len(a.AggregateExpr))
	for _, expr := range a.AggregateExpr {
		switch e := expr.(type) {
		case logicalplan.Avg:
			inputExpr, err := buildPhysicalExpr(e.Expr, inputSchema)
			if err != nil {
				return nil, err
			}
			aggregates = append(aggregates, AvgExpr{Expr: inputExpr})
		default:
			return nil, fmt.Errorf("unsupported aggregate expression: %T", expr)
		}
	}
	return HashAggregateExec{
		Input: input, Schema: outputSchema, GroupExpr: groupExprs, AggregateExpr: aggregates,
	}, nil
}

func buildProjection(p logicalplan.Projection) (PhysicalPlan, error) {
	input, err := BuildPhysicalPlan(p.Input)
	if err != nil {
		return nil, err
	}
	schema, err := p.Input.Schema()
	if err != nil {
		return nil, err
	}
	outputSchema, err := p.Schema()
	if err != nil {
		return nil, err
	}
	exprs := make([]PhysicalExpr, 0, len(p.Expr))
	for _, expr := range p.Expr {
		physicalExpr, err := buildPhysicalExpr(expr, schema)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, physicalExpr)
	}
	return ProjectionExec{Input: input, Schema: outputSchema, Expr: exprs}, nil
}

func buildPhysicalExpr(expr logicalplan.LogicalExpr, schema datasource.Schema) (PhysicalExpr, error) {
	var column logicalplan.Column
	switch e := expr.(type) {
	case logicalplan.ColumnIndex:
		if e.Index < 0 || e.Index >= len(schema.Fields) {
			return nil, fmt.Errorf("column index %d out of range", e.Index)
		}
		return ColumnExpr{Index: e.Index}, nil
	case logicalplan.Column:
		column = e
	default:
		return nil, fmt.Errorf("unsupported logical expression: %T", expr)
	}

	index := -1
	for i, field := range schema.Fields {
		if field.Name == column.Name && (column.Qualifier == "" || field.Qualifier == column.Qualifier) {
			if index >= 0 {
				return nil, fmt.Errorf("ambiguous column %q", column.String())
			}
			index = i
		}
	}
	if index < 0 {
		return nil, fmt.Errorf("no column named %q", column.String())
	}
	return ColumnExpr{Index: index}, nil
}
