// Package logicalplan translates SQL statements into logical operations.
package logicalplan

import (
	"fmt"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/parser"
)

// LogicalPlan is an operation in a logical query plan.
type LogicalPlan interface {
	Children() []LogicalPlan
	Schema() (datasource.Schema, error)
}

// LogicalExpr is an expression in a logical query plan.
type LogicalExpr interface {
	String() string
	ToField(input LogicalPlan) (datasource.Field, error)
}

// Scan represents a table that supplies rows.
type Scan struct {
	TableName string
	Source    datasource.DataSource
}

func (Scan) Children() []LogicalPlan { return nil }

func (s Scan) Schema() (datasource.Schema, error) { return s.Source.Schema(), nil }

// Projection evaluates expressions against its input.
type Projection struct {
	Input LogicalPlan
	Expr  []LogicalExpr
}

func (p Projection) Children() []LogicalPlan { return []LogicalPlan{p.Input} }

func (p Projection) Schema() (datasource.Schema, error) {
	fields := make([]datasource.Field, 0, len(p.Expr))
	for _, expr := range p.Expr {
		field, err := expr.ToField(p.Input)
		if err != nil {
			return datasource.Schema{}, err
		}
		fields = append(fields, field)
	}
	return datasource.Schema{Fields: fields}, nil
}

// Column references a column by name.
type Column struct {
	Name string
}

func (c Column) String() string { return c.Name }

func (c Column) ToField(input LogicalPlan) (datasource.Field, error) {
	schema, err := input.Schema()
	if err != nil {
		return datasource.Field{}, err
	}
	for _, field := range schema.Fields {
		if field.Name == c.Name {
			return field, nil
		}
	}
	return datasource.Field{}, fmt.Errorf("no column named %q", c.Name)
}

// BuildLogicalPlan translates a SQL statement into a logical plan.
func BuildLogicalPlan(stmt parser.SqlStatement, tables map[string]datasource.DataSource) (LogicalPlan, error) {
	switch query := stmt.(type) {
	case parser.SqlSelect:
		return buildSelect(query, tables)
	case *parser.SqlSelect:
		if query == nil {
			return nil, fmt.Errorf("nil SQL select statement")
		}
		return buildSelect(*query, tables)
	default:
		return nil, fmt.Errorf("unsupported SQL statement: %T", stmt)
	}
}

func buildSelect(query parser.SqlSelect, tables map[string]datasource.DataSource) (LogicalPlan, error) {
	source, ok := tables[query.TableName.ID]
	if !ok || source == nil {
		return nil, fmt.Errorf("no table named %q", query.TableName.ID)
	}
	exprs := make([]LogicalExpr, 0, len(query.Projection))
	for _, expr := range query.Projection {
		switch e := expr.(type) {
		case parser.SqlIdentifier:
			exprs = append(exprs, Column{Name: e.ID})
		default:
			return nil, fmt.Errorf("unsupported SQL expression: %T", expr)
		}
	}
	plan := Projection{
		Input: Scan{TableName: query.TableName.ID, Source: source},
		Expr:  exprs,
	}
	if _, err := plan.Schema(); err != nil {
		return nil, err
	}
	return plan, nil
}
