// Package planserde translates physical plans to and from protobuf messages.
package planserde

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/physicalplan"
)

// SerializePlan translates an executable plan into a portable description.
func SerializePlan(plan physicalplan.PhysicalPlan) (*Plan, error) {
	switch p := plan.(type) {
	case physicalplan.ScanExec:
		sourceSchema, err := encodeSchema(p.Source.Schema())
		if err != nil {
			return nil, err
		}
		var output *Schema
		if p.OutputSchema.Fields != nil {
			output, err = encodeSchema(p.OutputSchema)
			if err != nil {
				return nil, err
			}
		}
		scan := &Scan{OutputSchema: output}
		switch source := p.Source.(type) {
		case *datasource.CSVSource:
			scan.Csv = &CsvSource{Path: source.Path(), BatchSize: int64(source.BatchSize()), Schema: sourceSchema}
		case *datasource.ParquetSource:
			scan.Parquet = &ParquetSource{Path: source.Path(), Schema: sourceSchema, RowGroups: int64(source.PartitionCount())}
		default:
			return nil, fmt.Errorf("unsupported data source: %T", p.Source)
		}
		return &Plan{Node: &Plan_Scan{Scan: scan}}, nil
	case physicalplan.ProjectionExec:
		input, err := SerializePlan(p.Input)
		if err != nil {
			return nil, err
		}
		schema, err := encodeSchema(p.OutputSchema)
		if err != nil {
			return nil, err
		}
		exprs, err := encodeExprs(p.Expr)
		if err != nil {
			return nil, err
		}
		return &Plan{Node: &Plan_Projection{Projection: &Projection{Input: input, OutputSchema: schema, Expressions: exprs}}}, nil
	case physicalplan.HashJoinExec:
		if p.Type != logicalplan.LeftJoin {
			return nil, fmt.Errorf("unsupported join type: %v", p.Type)
		}
		left, err := SerializePlan(p.Left)
		if err != nil {
			return nil, err
		}
		right, err := SerializePlan(p.Right)
		if err != nil {
			return nil, err
		}
		schema, err := encodeSchema(p.OutputSchema)
		if err != nil {
			return nil, err
		}
		join := &HashJoin{Left: left, Right: right, OutputSchema: schema, Type: JoinType_LEFT}
		for _, key := range p.LeftKeys {
			join.LeftKeys = append(join.LeftKeys, int64(key))
		}
		for _, key := range p.RightKeys {
			join.RightKeys = append(join.RightKeys, int64(key))
		}
		return &Plan{Node: &Plan_HashJoin{HashJoin: join}}, nil
	case physicalplan.HashAggregateExec:
		input, err := SerializePlan(p.Input)
		if err != nil {
			return nil, err
		}
		schema, err := encodeSchema(p.OutputSchema)
		if err != nil {
			return nil, err
		}
		groups, err := encodeExprs(p.GroupExpr)
		if err != nil {
			return nil, err
		}
		a := &HashAggregate{Input: input, OutputSchema: schema, GroupExpressions: groups, Mode: AggregateMode(p.Mode)}
		for _, expr := range p.AggregateExpr {
			avg, ok := expr.(physicalplan.AvgExpr)
			if !ok {
				return nil, fmt.Errorf("unsupported aggregate expression: %T", expr)
			}
			arg, err := encodeExpr(avg.Expr)
			if err != nil {
				return nil, err
			}
			aggregate := &AggregateExpression{Node: &AggregateExpression_Avg{Avg: arg}}
			if avg.CountExpr != nil {
				aggregate.CountExpr, err = encodeExpr(avg.CountExpr)
				if err != nil {
					return nil, err
				}
			}
			a.AggregateExpressions = append(a.AggregateExpressions, aggregate)
		}
		return &Plan{Node: &Plan_HashAggregate{HashAggregate: a}}, nil
	case distributed.ExchangeWriteExec:
		input, err := SerializePlan(p.Input)
		if err != nil {
			return nil, err
		}
		return &Plan{Node: &Plan_ExchangeWrite{ExchangeWrite: &ExchangeWrite{Input: input, StageId: int64(p.StageID), KeyIndex: int64(p.KeyIndex)}}}, nil
	case distributed.ExchangeReadExec:
		schema, err := encodeSchema(p.OutputSchema)
		if err != nil {
			return nil, err
		}
		return &Plan{Node: &Plan_ExchangeRead{ExchangeRead: &ExchangeRead{StageId: int64(p.StageID), OutputSchema: schema}}}, nil
	default:
		return nil, fmt.Errorf("unsupported physical plan: %T", plan)
	}
}

func encodeSchema(schema datasource.Schema) (*Schema, error) {
	out := &Schema{}
	for _, f := range schema.Fields {
		var typ DataType
		if f.Type == nil {
			return nil, fmt.Errorf("missing type for field %q", f.Name)
		}
		switch f.Type.ID() {
		case arrow.INT64:
			typ = DataType_INT64
		case arrow.FLOAT64:
			typ = DataType_FLOAT64
		case arrow.STRING:
			typ = DataType_STRING
		case arrow.BOOL:
			typ = DataType_BOOL
		default:
			return nil, fmt.Errorf("unsupported field type: %s", f.Type)
		}
		out.Fields = append(out.Fields, &Field{Name: f.Name, Qualifier: f.Qualifier, Type: typ})
	}
	return out, nil
}

func encodeExprs(exprs []physicalplan.PhysicalExpr) ([]*Expression, error) {
	var out []*Expression
	for _, expr := range exprs {
		e, err := encodeExpr(expr)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
func encodeExpr(expr physicalplan.PhysicalExpr) (*Expression, error) {
	switch e := expr.(type) {
	case physicalplan.ColumnExpr:
		return &Expression{Node: &Expression_Column{Column: &Column{Index: int64(e.Index)}}}, nil
	default:
		return nil, fmt.Errorf("unsupported physical expression: %T", expr)
	}
}

// DeserializePlan rebuilds a physical plan and its data sources on a worker.
func DeserializePlan(plan *Plan) (physicalplan.PhysicalPlan, error) {
	if plan == nil {
		return nil, fmt.Errorf("missing plan")
	}
	switch p := plan.Node.(type) {
	case *Plan_Scan:
		if p.Scan == nil {
			return nil, fmt.Errorf("missing scan")
		}
		var source datasource.DataSource
		var expectedSchema *Schema
		var err error
		switch {
		case p.Scan.Csv != nil && p.Scan.Parquet == nil:
			source, err = datasource.NewCSVSource(p.Scan.Csv.Path, int(p.Scan.Csv.BatchSize))
			expectedSchema = p.Scan.Csv.Schema
		case p.Scan.Parquet != nil && p.Scan.Csv == nil:
			source, err = datasource.NewParquetSource(p.Scan.Parquet.Path)
			expectedSchema = p.Scan.Parquet.Schema
			if err == nil && int64(source.PartitionCount()) != p.Scan.Parquet.RowGroups {
				return nil, fmt.Errorf("Parquet row group count changed")
			}
		default:
			return nil, fmt.Errorf("scan requires exactly one data source")
		}
		if err != nil {
			return nil, err
		}
		expected, err := decodeSchema(expectedSchema)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(source.Schema(), expected) {
			return nil, fmt.Errorf("data source schema changed")
		}
		var output datasource.Schema
		if p.Scan.OutputSchema != nil {
			output, err = decodeSchema(p.Scan.OutputSchema)
			if err != nil {
				return nil, err
			}
		}
		return physicalplan.ScanExec{Source: source, OutputSchema: output}, nil
	case *Plan_Projection:
		if p.Projection == nil {
			return nil, fmt.Errorf("missing projection")
		}
		x := p.Projection
		input, err := DeserializePlan(x.Input)
		if err != nil {
			return nil, err
		}
		schema, err := decodeSchema(x.OutputSchema)
		if err != nil {
			return nil, err
		}
		exprs, err := decodeExprs(x.Expressions)
		if err != nil {
			return nil, err
		}
		return physicalplan.ProjectionExec{Input: input, OutputSchema: schema, Expr: exprs}, nil
	case *Plan_HashJoin:
		if p.HashJoin == nil {
			return nil, fmt.Errorf("missing hash join")
		}
		x := p.HashJoin
		if x.Type != JoinType_LEFT {
			return nil, fmt.Errorf("unsupported join type: %v", x.Type)
		}
		left, err := DeserializePlan(x.Left)
		if err != nil {
			return nil, err
		}
		right, err := DeserializePlan(x.Right)
		if err != nil {
			return nil, err
		}
		schema, err := decodeSchema(x.OutputSchema)
		if err != nil {
			return nil, err
		}
		join := physicalplan.HashJoinExec{Left: left, Right: right, OutputSchema: schema, Type: logicalplan.LeftJoin}
		for _, key := range x.LeftKeys {
			join.LeftKeys = append(join.LeftKeys, int(key))
		}
		for _, key := range x.RightKeys {
			join.RightKeys = append(join.RightKeys, int(key))
		}
		return join, nil
	case *Plan_HashAggregate:
		if p.HashAggregate == nil {
			return nil, fmt.Errorf("missing hash aggregate")
		}
		x := p.HashAggregate
		input, err := DeserializePlan(x.Input)
		if err != nil {
			return nil, err
		}
		schema, err := decodeSchema(x.OutputSchema)
		if err != nil {
			return nil, err
		}
		groups, err := decodeExprs(x.GroupExpressions)
		if err != nil {
			return nil, err
		}
		a := physicalplan.HashAggregateExec{Input: input, OutputSchema: schema, GroupExpr: groups, Mode: physicalplan.AggregateMode(x.Mode)}
		for _, expr := range x.AggregateExpressions {
			if expr == nil {
				return nil, fmt.Errorf("missing aggregate expression")
			}
			switch e := expr.Node.(type) {
			case *AggregateExpression_Avg:
				arg, err := decodeExpr(e.Avg)
				if err != nil {
					return nil, err
				}
				avg := physicalplan.AvgExpr{Expr: arg}
				if expr.CountExpr != nil {
					avg.CountExpr, err = decodeExpr(expr.CountExpr)
					if err != nil {
						return nil, err
					}
				}
				a.AggregateExpr = append(a.AggregateExpr, avg)
			default:
				return nil, fmt.Errorf("unsupported aggregate expression: %T", expr.Node)
			}
		}
		return a, nil
	case *Plan_ExchangeWrite:
		if p.ExchangeWrite == nil {
			return nil, fmt.Errorf("missing exchange write")
		}
		x := p.ExchangeWrite
		input, err := DeserializePlan(x.Input)
		if err != nil {
			return nil, err
		}
		return distributed.ExchangeWriteExec{Input: input, StageID: int(x.StageId), KeyIndex: int(x.KeyIndex)}, nil
	case *Plan_ExchangeRead:
		if p.ExchangeRead == nil {
			return nil, fmt.Errorf("missing exchange read")
		}
		x := p.ExchangeRead
		schema, err := decodeSchema(x.OutputSchema)
		if err != nil {
			return nil, err
		}
		return distributed.ExchangeReadExec{StageID: int(x.StageId), OutputSchema: schema}, nil
	default:
		return nil, fmt.Errorf("unsupported plan node: %T", plan.Node)
	}
}

func decodeSchema(schema *Schema) (datasource.Schema, error) {
	if schema == nil {
		return datasource.Schema{}, fmt.Errorf("missing schema")
	}
	out := datasource.Schema{Fields: make([]datasource.Field, 0, len(schema.Fields))}
	for _, f := range schema.Fields {
		if f == nil {
			return datasource.Schema{}, fmt.Errorf("missing field")
		}
		var typ arrow.DataType
		switch f.Type {
		case DataType_INT64:
			typ = arrow.PrimitiveTypes.Int64
		case DataType_FLOAT64:
			typ = arrow.PrimitiveTypes.Float64
		case DataType_STRING:
			typ = arrow.BinaryTypes.String
		case DataType_BOOL:
			typ = arrow.FixedWidthTypes.Boolean
		default:
			return datasource.Schema{}, fmt.Errorf("unsupported field type: %v", f.Type)
		}
		out.Fields = append(out.Fields, datasource.Field{Name: f.Name, Qualifier: f.Qualifier, Type: typ})
	}
	return out, nil
}
func decodeExprs(exprs []*Expression) ([]physicalplan.PhysicalExpr, error) {
	var out []physicalplan.PhysicalExpr
	for _, expr := range exprs {
		e, err := decodeExpr(expr)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
func decodeExpr(expr *Expression) (physicalplan.PhysicalExpr, error) {
	if expr == nil {
		return nil, fmt.Errorf("missing expression")
	}
	switch e := expr.Node.(type) {
	case *Expression_Column:
		if e.Column == nil {
			return nil, fmt.Errorf("missing column")
		}
		return physicalplan.ColumnExpr{Index: int(e.Column.Index)}, nil
	default:
		return nil, fmt.Errorf("unsupported expression: %T", expr.Node)
	}
}
