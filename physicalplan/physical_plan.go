// Package physicalplan builds physical operations from logical plans.
package physicalplan

import (
	"fmt"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
)

// PhysicalPlan is an operation in a physical query plan.
type PhysicalPlan interface {
	Children() []PhysicalPlan
}

// PhysicalExpr is an expression in a physical query plan.
type PhysicalExpr interface {
	String() string
}

// ScanExec represents a scan of a data source.
type ScanExec struct {
	Source datasource.DataSource
}

func (ScanExec) Children() []PhysicalPlan { return nil }

// ProjectionExec represents a projection over an input plan.
type ProjectionExec struct {
	Input PhysicalPlan
	Expr  []PhysicalExpr
}

func (p ProjectionExec) Children() []PhysicalPlan { return []PhysicalPlan{p.Input} }

// ColumnExpr refers to a column by its position in the input schema.
type ColumnExpr struct {
	Index int
}

func (c ColumnExpr) String() string { return fmt.Sprintf("column[%d]", c.Index) }

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
	default:
		return nil, fmt.Errorf("unsupported logical plan: %T", plan)
	}
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
	exprs := make([]PhysicalExpr, 0, len(p.Expr))
	for _, expr := range p.Expr {
		physicalExpr, err := buildPhysicalExpr(expr, schema)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, physicalExpr)
	}
	return ProjectionExec{Input: input, Expr: exprs}, nil
}

func buildPhysicalExpr(expr logicalplan.LogicalExpr, schema datasource.Schema) (PhysicalExpr, error) {
	var column logicalplan.Column
	switch e := expr.(type) {
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
