// Package logicalplan translates SQL statements into logical operations.
package logicalplan

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

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
	Columns   []string // nil selects all source columns
}

func (Scan) Children() []LogicalPlan { return nil }

func (s Scan) Schema() (datasource.Schema, error) {
	schema := s.Source.Schema()
	fields := append([]datasource.Field(nil), schema.Fields...)
	if s.Columns != nil {
		if len(s.Columns) == 0 {
			return datasource.Schema{}, fmt.Errorf("scan requires at least one column")
		}
		fields = make([]datasource.Field, 0, len(s.Columns))
		seen := make(map[string]bool)
		for _, name := range s.Columns {
			if seen[name] {
				return datasource.Schema{}, fmt.Errorf("duplicate scan column %q", name)
			}
			seen[name] = true
			found := false
			for _, field := range schema.Fields {
				if field.Name == name {
					fields = append(fields, field)
					found = true
					break
				}
			}
			if !found {
				return datasource.Schema{}, fmt.Errorf("no column named %q", name)
			}
		}
	}
	for i := range fields {
		fields[i].Qualifier = s.TableName
	}
	return datasource.Schema{Fields: fields}, nil
}

// JoinType describes how a join handles unmatched rows.
type JoinType uint8

const LeftJoin JoinType = 1

// JoinKey identifies the matching columns in the left and right inputs.
type JoinKey struct {
	Left  string
	Right string
}

// Join combines rows from two inputs using equality keys.
type Join struct {
	Left  LogicalPlan
	Right LogicalPlan
	Type  JoinType
	On    []JoinKey
}

func (j Join) Children() []LogicalPlan { return []LogicalPlan{j.Left, j.Right} }

func (j Join) Schema() (datasource.Schema, error) {
	left, err := j.Left.Schema()
	if err != nil {
		return datasource.Schema{}, err
	}
	right, err := j.Right.Schema()
	if err != nil {
		return datasource.Schema{}, err
	}
	fields := make([]datasource.Field, 0, len(left.Fields)+len(right.Fields))
	fields = append(fields, left.Fields...)
	fields = append(fields, right.Fields...)
	return datasource.Schema{Fields: fields}, nil
}

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
	Qualifier string
	Name      string
}

func (c Column) String() string {
	if c.Qualifier != "" {
		return c.Qualifier + "." + c.Name
	}
	return c.Name
}

func (c Column) ToField(input LogicalPlan) (datasource.Field, error) {
	schema, err := input.Schema()
	if err != nil {
		return datasource.Field{}, err
	}
	var match *datasource.Field
	for i := range schema.Fields {
		field := &schema.Fields[i]
		if field.Name == c.Name && (c.Qualifier == "" || field.Qualifier == c.Qualifier) {
			if match != nil {
				return datasource.Field{}, fmt.Errorf("ambiguous column %q", c.String())
			}
			match = field
		}
	}
	if match == nil {
		return datasource.Field{}, fmt.Errorf("no column named %q", c.String())
	}
	return *match, nil
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
	if field.Type == nil {
		return datasource.Field{}, fmt.Errorf("AVG input has no type")
	}
	switch field.Type.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64:
		return datasource.Field{Name: "AVG", Type: arrow.PrimitiveTypes.Float64}, nil
	default:
		return datasource.Field{}, fmt.Errorf("AVG does not support type %s", field.Type)
	}
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
	scan, err := buildScan(query.TableName.ID, tables)
	if err != nil {
		return nil, err
	}
	var input LogicalPlan = scan
	if query.Join != nil {
		input, err = buildJoin(scan, *query.Join, tables)
		if err != nil {
			return nil, err
		}
	}
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
		plan, err = buildNonAggregateProjection(input, query.Projection)
	} else {
		aggregate := Aggregate{Input: input, GroupExpr: groupExprs, AggregateExpr: aggregates}
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

func buildScan(name string, tables map[string]datasource.DataSource) (Scan, error) {
	source, ok := tables[name]
	if !ok || source == nil {
		return Scan{}, fmt.Errorf("no table named %q", name)
	}
	return Scan{TableName: name, Source: source}, nil
}

func buildJoin(left Scan, clause parser.SqlJoin, tables map[string]datasource.DataSource) (LogicalPlan, error) {
	if clause.Type != parser.LeftJoin {
		return nil, fmt.Errorf("unsupported join type %q", clause.Type)
	}
	right, err := buildScan(clause.TableName.ID, tables)
	if err != nil {
		return nil, err
	}
	if left.TableName == right.TableName {
		return nil, fmt.Errorf("joining a table to itself requires aliases, which are not supported")
	}
	firstSide, err := resolveJoinKey(clause.LeftKey, left, right)
	if err != nil {
		return nil, err
	}
	secondSide, err := resolveJoinKey(clause.RightKey, left, right)
	if err != nil {
		return nil, err
	}
	if firstSide == secondSide {
		return nil, fmt.Errorf("join keys must reference opposite tables")
	}
	leftKey, rightKey := clause.LeftKey.ID, clause.RightKey.ID
	if firstSide == right.TableName {
		leftKey, rightKey = rightKey, leftKey
	}
	return Join{Left: left, Right: right, Type: LeftJoin, On: []JoinKey{{Left: leftKey, Right: rightKey}}}, nil
}

func resolveJoinKey(key parser.SqlIdentifier, left, right Scan) (string, error) {
	var matching []string
	for _, scan := range []Scan{left, right} {
		if key.Qualifier != "" && key.Qualifier != scan.TableName {
			continue
		}
		for _, field := range scan.Source.Schema().Fields {
			if field.Name == key.ID {
				matching = append(matching, scan.TableName)
			}
		}
	}
	column := Column{Qualifier: key.Qualifier, Name: key.ID}
	if len(matching) == 0 {
		return "", fmt.Errorf("no column named %q", column.String())
	}
	if len(matching) > 1 {
		return "", fmt.Errorf("ambiguous column %q", column.String())
	}
	return matching[0], nil
}

func buildGroupExpressions(groupBy []parser.SqlExpr) ([]LogicalExpr, error) {
	var groupExprs []LogicalExpr
	for _, expr := range groupBy {
		identifier, ok := expr.(parser.SqlIdentifier)
		if !ok {
			return nil, fmt.Errorf("unsupported GROUP BY expression: %T", expr)
		}
		groupExprs = append(groupExprs, Column{Qualifier: identifier.Qualifier, Name: identifier.ID})
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
			aggregates = append(aggregates, Avg{Expr: Column{Qualifier: arg.Qualifier, Name: arg.ID}})
		}
	}
	return aggregates, nil
}

func buildNonAggregateProjection(input LogicalPlan, projection []parser.SqlExpr) (Projection, error) {
	exprs := make([]LogicalExpr, 0, len(projection))
	for _, expr := range projection {
		column, ok := expr.(parser.SqlIdentifier)
		if !ok {
			return Projection{}, fmt.Errorf("unsupported SQL expression: %T", expr)
		}
		exprs = append(exprs, Column{Qualifier: column.Qualifier, Name: column.ID})
	}
	return Projection{Input: input, Expr: exprs}, nil
}

func buildAggregateProjection(aggregate Aggregate, projection []parser.SqlExpr) (Projection, error) {
	exprs := make([]LogicalExpr, 0, len(projection))
	aggregateIndex := 0
	for _, expr := range projection {
		switch e := expr.(type) {
		case parser.SqlIdentifier:
			index := -1
			for i, groupExpr := range aggregate.GroupExpr {
				if groupExpr.(Column) == (Column{Qualifier: e.Qualifier, Name: e.ID}) {
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
