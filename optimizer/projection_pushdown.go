package optimizer

import (
	"fmt"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
)

// ProjectionPushDownRule restricts scans to columns needed by their consumers.
type ProjectionPushDownRule struct{}

func (ProjectionPushDownRule) Optimize(plan logicalplan.LogicalPlan) (logicalplan.LogicalPlan, error) {
	schema, err := plan.Schema()
	if err != nil {
		return nil, err
	}
	required := make(columnSet)
	for i := range schema.Fields {
		required[i] = true
	}
	result, err := pushDown(plan, required)
	return result.plan, err
}

type columnSet map[int]bool

// positions maps each original output position to its position after rewriting.
// Removed columns have position -1.
type rewriteResult struct {
	plan      logicalplan.LogicalPlan
	positions []int
}

func identityPositions(count int) []int {
	positions := make([]int, count)
	for i := range positions {
		positions[i] = i
	}
	return positions
}

func pushDown(plan logicalplan.LogicalPlan, required columnSet) (rewriteResult, error) {
	switch p := plan.(type) {
	case logicalplan.Scan:
		return pruneScan(p, required)
	case logicalplan.Projection:
		return pushProjection(p)
	case logicalplan.Aggregate:
		return pushAggregate(p)
	case logicalplan.Join:
		return pushJoin(p, required)
	default:
		return rewriteResult{}, fmt.Errorf("unsupported projection pushdown plan: %T", plan)
	}
}

func pruneScan(scan logicalplan.Scan, required columnSet) (rewriteResult, error) {
	schema, err := scan.Schema()
	if err != nil {
		return rewriteResult{}, err
	}
	positions := identityPositions(len(schema.Fields))
	if len(required) == 0 || len(required) == len(schema.Fields) {
		return rewriteResult{plan: scan, positions: positions}, nil
	}
	columns := make([]string, 0, len(required))
	for i, field := range schema.Fields {
		positions[i] = -1
		if required[i] {
			positions[i] = len(columns)
			columns = append(columns, field.Name)
		}
	}
	scan.Columns = columns
	return rewriteResult{plan: scan, positions: positions}, nil
}

func pushProjection(projection logicalplan.Projection) (rewriteResult, error) {
	required, err := requiredColumns(projection.Expr, projection.Input)
	if err != nil {
		return rewriteResult{}, err
	}
	input, err := pushDown(projection.Input, required)
	if err != nil {
		return rewriteResult{}, err
	}
	projection.Input = input.plan
	projection.Expr = remapExpressions(projection.Expr, input.positions)
	return rewriteResult{plan: projection, positions: identityPositions(len(projection.Expr))}, nil
}

func pushAggregate(aggregate logicalplan.Aggregate) (rewriteResult, error) {
	exprs := append([]logicalplan.LogicalExpr(nil), aggregate.GroupExpr...)
	for _, expr := range aggregate.AggregateExpr {
		switch a := expr.(type) {
		case logicalplan.Avg:
			exprs = append(exprs, a.Expr)
		default:
			return rewriteResult{}, fmt.Errorf("unsupported projection pushdown aggregate: %T", expr)
		}
	}
	required, err := requiredColumns(exprs, aggregate.Input)
	if err != nil {
		return rewriteResult{}, err
	}
	input, err := pushDown(aggregate.Input, required)
	if err != nil {
		return rewriteResult{}, err
	}
	aggregate.Input = input.plan
	aggregate.GroupExpr = remapExpressions(aggregate.GroupExpr, input.positions)
	aggregates := make([]logicalplan.AggregateExpr, len(aggregate.AggregateExpr))
	for i, expr := range aggregate.AggregateExpr {
		a := expr.(logicalplan.Avg)
		a.Expr = remapExpression(a.Expr, input.positions)
		aggregates[i] = a
	}
	aggregate.AggregateExpr = aggregates
	return rewriteResult{plan: aggregate, positions: identityPositions(len(aggregate.GroupExpr) + len(aggregate.AggregateExpr))}, nil
}

func pushJoin(join logicalplan.Join, required columnSet) (rewriteResult, error) {
	leftSchema, err := join.Left.Schema()
	if err != nil {
		return rewriteResult{}, err
	}
	rightSchema, err := join.Right.Schema()
	if err != nil {
		return rewriteResult{}, err
	}
	leftRequired, rightRequired := make(columnSet), make(columnSet)
	for index := range required {
		if index < len(leftSchema.Fields) {
			leftRequired[index] = true
		} else {
			rightRequired[index-len(leftSchema.Fields)] = true
		}
	}
	for _, key := range join.On {
		leftIndex, err := expressionIndex(logicalplan.Column{Name: key.Left}, leftSchema)
		if err != nil {
			return rewriteResult{}, err
		}
		rightIndex, err := expressionIndex(logicalplan.Column{Name: key.Right}, rightSchema)
		if err != nil {
			return rewriteResult{}, err
		}
		leftRequired[leftIndex], rightRequired[rightIndex] = true, true
	}
	left, err := pushDown(join.Left, leftRequired)
	if err != nil {
		return rewriteResult{}, err
	}
	right, err := pushDown(join.Right, rightRequired)
	if err != nil {
		return rewriteResult{}, err
	}
	newLeftSchema, err := left.plan.Schema()
	if err != nil {
		return rewriteResult{}, err
	}
	positions := append([]int(nil), left.positions...)
	for _, index := range right.positions {
		if index >= 0 {
			index += len(newLeftSchema.Fields)
		}
		positions = append(positions, index)
	}
	join.Left, join.Right = left.plan, right.plan
	return rewriteResult{plan: join, positions: positions}, nil
}

func requiredColumns(exprs []logicalplan.LogicalExpr, input logicalplan.LogicalPlan) (columnSet, error) {
	schema, err := input.Schema()
	if err != nil {
		return nil, err
	}
	required := make(columnSet)
	for _, expr := range exprs {
		index, err := expressionIndex(expr, schema)
		if err != nil {
			return nil, err
		}
		required[index] = true
	}
	return required, nil
}

func expressionIndex(expr logicalplan.LogicalExpr, schema datasource.Schema) (int, error) {
	switch column := expr.(type) {
	case logicalplan.ColumnIndex:
		if column.Index < 0 || column.Index >= len(schema.Fields) {
			return 0, fmt.Errorf("column index %d out of range", column.Index)
		}
		return column.Index, nil
	case logicalplan.Column:
		index := -1
		for i, field := range schema.Fields {
			if field.Name == column.Name && (column.Qualifier == "" || column.Qualifier == field.Qualifier) {
				if index >= 0 {
					return 0, fmt.Errorf("ambiguous column %q", column.String())
				}
				index = i
			}
		}
		if index < 0 {
			return 0, fmt.Errorf("no column named %q", column.String())
		}
		return index, nil
	default:
		return 0, fmt.Errorf("unsupported projection pushdown expression: %T", expr)
	}
}

func remapExpressions(exprs []logicalplan.LogicalExpr, positions []int) []logicalplan.LogicalExpr {
	if exprs == nil {
		return nil
	}
	result := make([]logicalplan.LogicalExpr, len(exprs))
	for i, expr := range exprs {
		result[i] = remapExpression(expr, positions)
	}
	return result
}

func remapExpression(expr logicalplan.LogicalExpr, positions []int) logicalplan.LogicalExpr {
	if column, ok := expr.(logicalplan.ColumnIndex); ok {
		return logicalplan.ColumnIndex{Index: positions[column.Index]}
	}
	return expr
}
