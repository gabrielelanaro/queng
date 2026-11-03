// Package logicalplan translates SQL statements into logical operations.
package logicalplan

import (
	"fmt"

	"github.com/gabrielelanaro/queng/parser"
)

// LogicalPlan is an operation in a logical query plan.
type LogicalPlan interface {
	Children() []LogicalPlan
}

// LogicalExpr is an expression in a logical query plan.
type LogicalExpr interface {
	String() string
}

// Scan represents a table that supplies rows.
type Scan struct {
	TableName string
}

func (Scan) Children() []LogicalPlan { return nil }

// Projection evaluates expressions against its input.
type Projection struct {
	Input LogicalPlan
	Expr  []LogicalExpr
}

func (p Projection) Children() []LogicalPlan { return []LogicalPlan{p.Input} }

// Column references a column by name.
type Column struct {
	Name string
}

func (c Column) String() string { return c.Name }

// BuildLogicalPlan translates a SQL statement into an unresolved logical plan.
func BuildLogicalPlan(stmt parser.SqlStatement) (LogicalPlan, error) {
	switch query := stmt.(type) {
	case parser.SqlSelect:
		return buildSelect(query)
	case *parser.SqlSelect:
		if query == nil {
			return nil, fmt.Errorf("nil SQL select statement")
		}
		return buildSelect(*query)
	default:
		return nil, fmt.Errorf("unsupported SQL statement: %T", stmt)
	}
}

func buildSelect(query parser.SqlSelect) (LogicalPlan, error) {
	exprs := make([]LogicalExpr, 0, len(query.Projection))
	for _, expr := range query.Projection {
		switch e := expr.(type) {
		case parser.SqlIdentifier:
			exprs = append(exprs, Column{Name: e.ID})
		default:
			return nil, fmt.Errorf("unsupported SQL expression: %T", expr)
		}
	}
	return Projection{
		Input: Scan{TableName: query.TableName.ID},
		Expr:  exprs,
	}, nil
}
