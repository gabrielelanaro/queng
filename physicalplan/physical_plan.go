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

// PhysicalExpr is an expression in a physical query plan.
type PhysicalExpr interface {
	String() string
	Evaluate(batch datasource.RecordBatch) ([]any, error)
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
					yield(datasource.RecordBatch{}, err)
					return
				}
				result.Columns = append(result.Columns, column)
			}
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

// ColumnExpr refers to a column by its position in the input schema.
type ColumnExpr struct {
	Index int
}

func (c ColumnExpr) String() string { return fmt.Sprintf("column[%d]", c.Index) }

func (c ColumnExpr) Evaluate(batch datasource.RecordBatch) ([]any, error) {
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
	default:
		return nil, fmt.Errorf("unsupported logical plan: %T", plan)
	}
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

	for i, field := range schema.Fields {
		if field.Name == column.Name {
			return ColumnExpr{Index: i}, nil
		}
	}
	return nil, fmt.Errorf("no column named %q", column.Name)
}
