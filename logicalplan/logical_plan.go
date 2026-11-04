// Package logicalplan translates SQL statements into logical operations.
package logicalplan

import (
	"fmt"
	"strings"

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

// AggregateExpr is a logical aggregate expression.
type AggregateExpr interface {
	LogicalExpr
	aggregateExpr()
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

// Aggregate groups input rows and calculates aggregate expressions for each group.
type Aggregate struct {
	Input         LogicalPlan
	GroupExpr     []LogicalExpr
	AggregateExpr []AggregateExpr
}

func (a Aggregate) Children() []LogicalPlan { return []LogicalPlan{a.Input} }

func (a Aggregate) Schema() (datasource.Schema, error) {
	var schema datasource.Schema
	for _, expr := range a.GroupExpr {
		field, err := expr.ToField(a.Input)
		if err != nil {
			return datasource.Schema{}, err
		}
		schema.Fields = append(schema.Fields, field)
	}
	for _, expr := range a.AggregateExpr {
		field, err := expr.ToField(a.Input)
		if err != nil {
			return datasource.Schema{}, err
		}
		schema.Fields = append(schema.Fields, field)
	}
	return schema, nil
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

// ColumnIndex references a column by its position in the input plan.
type ColumnIndex struct {
	Index int
}

func (c ColumnIndex) String() string { return fmt.Sprintf("#%d", c.Index) }

func (c ColumnIndex) ToField(input LogicalPlan) (datasource.Field, error) {
	schema, err := input.Schema()
	if err != nil {
		return datasource.Field{}, err
	}
	if c.Index < 0 || c.Index >= len(schema.Fields) {
		return datasource.Field{}, fmt.Errorf("column index %d out of range", c.Index)
	}
	return schema.Fields[c.Index], nil
}

// Avg describes the average of an expression.
type Avg struct {
	Expr LogicalExpr
}

func (Avg) aggregateExpr() {}

func (a Avg) String() string { return fmt.Sprintf("AVG(%s)", a.Expr) }

func (a Avg) ToField(input LogicalPlan) (datasource.Field, error) {
	field, err := a.Expr.ToField(input)
	if err != nil {
		return datasource.Field{}, err
	}
	return datasource.Field{Name: "AVG", Type: field.Type}, nil
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
	scan := Scan{TableName: query.TableName.ID, Source: source}
	groupExprs, err := buildGroupExpressions(query.GroupBy)
	if err != nil {
		return nil, err
	}
	aggregates, err := buildAggregateExpressions(query.Projection)
	if err != nil {
		return nil, err
	}
	if len(groupExprs) > 0 && len(aggregates) == 0 {
		return nil, fmt.Errorf("GROUP BY without aggregate expressions is not supported")
	}
	var plan Projection
	if len(aggregates) == 0 {
		plan, err = buildScanProjection(scan, query.Projection)
	} else {
		aggregate := Aggregate{Input: scan, GroupExpr: groupExprs, AggregateExpr: aggregates}
		plan, err = buildAggregateProjection(aggregate, query.Projection)
	}
	if err != nil {
		return nil, err
	}
	if _, err := plan.Schema(); err != nil {
		return nil, err
	}
	return plan, nil
}

func buildGroupExpressions(groupBy []parser.SqlExpr) ([]LogicalExpr, error) {
	var groupExprs []LogicalExpr
	for _, expr := range groupBy {
		identifier, ok := expr.(parser.SqlIdentifier)
		if !ok {
			return nil, fmt.Errorf("unsupported GROUP BY expression: %T", expr)
		}
		groupExprs = append(groupExprs, Column{Name: identifier.ID})
	}
	return groupExprs, nil
}

func buildAggregateExpressions(projection []parser.SqlExpr) ([]AggregateExpr, error) {
	var aggregates []AggregateExpr
	for _, expr := range projection {
		if function, ok := expr.(parser.SqlFunction); ok {
			if !strings.EqualFold(function.ID, "AVG") || len(function.Args) != 1 {
				return nil, fmt.Errorf("unsupported SQL function: %s", function.ID)
			}
			arg, ok := function.Args[0].(parser.SqlIdentifier)
			if !ok {
				return nil, fmt.Errorf("unsupported AVG argument: %T", function.Args[0])
			}
			aggregates = append(aggregates, Avg{Expr: Column{Name: arg.ID}})
		}
	}
	return aggregates, nil
}

func buildScanProjection(scan Scan, projection []parser.SqlExpr) (Projection, error) {
	exprs := make([]LogicalExpr, 0, len(projection))
	for _, expr := range projection {
		column, ok := expr.(parser.SqlIdentifier)
		if !ok {
			return Projection{}, fmt.Errorf("unsupported SQL expression: %T", expr)
		}
		exprs = append(exprs, Column{Name: column.ID})
	}
	return Projection{Input: scan, Expr: exprs}, nil
}

func buildAggregateProjection(aggregate Aggregate, projection []parser.SqlExpr) (Projection, error) {
	exprs := make([]LogicalExpr, 0, len(projection))
	aggregateIndex := 0
	for _, expr := range projection {
		switch e := expr.(type) {
		case parser.SqlIdentifier:
			index := -1
			for i, groupExpr := range aggregate.GroupExpr {
				if groupExpr.(Column).Name == e.ID {
					index = i
					break
				}
			}
			if index < 0 {
				return Projection{}, fmt.Errorf("column %q must appear in GROUP BY", e.ID)
			}
			exprs = append(exprs, ColumnIndex{Index: index})
		case parser.SqlFunction:
			exprs = append(exprs, ColumnIndex{Index: len(aggregate.GroupExpr) + aggregateIndex})
			aggregateIndex++
		default:
			return Projection{}, fmt.Errorf("unsupported SQL expression: %T", expr)
		}
	}
	return Projection{Input: aggregate, Expr: exprs}, nil
}
