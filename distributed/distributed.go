// Package distributed plans and executes distributed queries.
package distributed

import (
	"context"
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog/log"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/physicalplan"
)

// Stage describes one part of a distributed physical plan.
type Stage struct {
	ID           int
	Plan         physicalplan.PhysicalPlan
	Dependencies []int
}

// ExchangeWriteExec partitions its input for downstream stages.
type ExchangeWriteExec struct {
	Input        physicalplan.PhysicalPlan
	StageID      int
	Partitioning Partitioning
}

func (e ExchangeWriteExec) Schema() datasource.Schema { return e.Input.Schema() }

func (e ExchangeWriteExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{e.Input}
}

func (e ExchangeWriteExec) Execute(ctx context.Context, env execution.Environment, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		if env.Exchange == nil {
			yield(datasource.RecordBatch{}, fmt.Errorf("exchange store is not configured"))
			return
		}
		for batch, err := range e.Input.Execute(ctx, env, partitionID) {
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
			err = e.writeBatch(env, batch)
			batch.Release()
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
		}
	}
}

// writeBatch groups rows by output partition and writes each group as a batch.
func (e ExchangeWriteExec) writeBatch(env execution.Environment, batch datasource.RecordBatch) error {
	partitionCount := e.Partitioning.PartitionCount(env.ShufflePartitions)
	if partitionCount == 1 {
		key := execution.ExchangeKey{JobID: env.JobID, StageID: e.StageID, TaskID: env.TaskID, PartitionID: 0}
		return env.Exchange.Write(key, batch)
	}
	rowsByPartition := make([][]int, partitionCount)
	for row := range batch.Column(0).Len() {
		partition, err := e.Partitioning.partitionForRow(batch, row, partitionCount)
		if err != nil {
			return err
		}
		rowsByPartition[partition] = append(rowsByPartition[partition], row)
	}
	for partitionID, rows := range rowsByPartition {
		if len(rows) == 0 {
			continue
		}
		output, err := batch.Take(rows)
		if err != nil {
			return err
		}
		key := execution.ExchangeKey{JobID: env.JobID, StageID: e.StageID, TaskID: env.TaskID, PartitionID: partitionID}
		err = env.Exchange.Write(key, output)
		output.Release()
		if err != nil {
			return err
		}
		log.Info().Str("job", env.JobID).Int("stage", e.StageID).Int("task", env.TaskID).Int("partition", partitionID).Int("rows", len(rows)).Msg("shuffle batch written")
	}
	return nil
}

// ExchangeReadExec reads the partition produced by an upstream stage.
type ExchangeReadExec struct {
	StageID      int
	OutputSchema datasource.Schema
	Partitioning Partitioning
}

func (e ExchangeReadExec) Schema() datasource.Schema { return e.OutputSchema }

func (ExchangeReadExec) Children() []physicalplan.PhysicalPlan { return nil }

func (e ExchangeReadExec) Execute(ctx context.Context, env execution.Environment, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	if env.Exchange == nil {
		return func(yield func(datasource.RecordBatch, error) bool) {
			yield(datasource.RecordBatch{}, fmt.Errorf("exchange store is not configured"))
		}
	}
	return env.Exchange.ReadPartition(ctx, env.JobID, e.StageID, partitionID)
}

// BuildStages splits a physical plan at join inputs, in dependency order.
func BuildStages(plan physicalplan.PhysicalPlan) ([]Stage, error) {
	builder := stageBuilder{}
	root, err := builder.buildPlan(plan)
	if err != nil {
		return nil, err
	}
	builder.addStage(root)
	return builder.stages, nil
}

type stageBuilder struct {
	stages []Stage
}

func (b *stageBuilder) addStage(plan physicalplan.PhysicalPlan) int {
	id := len(b.stages)
	b.stages = append(b.stages, Stage{ID: id, Plan: plan, Dependencies: stageDependencies(plan)})
	return id
}

func stageDependencies(plan physicalplan.PhysicalPlan) []int {
	var dependencies []int
	if read, ok := plan.(ExchangeReadExec); ok {
		dependencies = append(dependencies, read.StageID)
	}
	for _, child := range plan.Children() {
		dependencies = append(dependencies, stageDependencies(child)...)
	}
	return dependencies
}

func (b *stageBuilder) addStageWithID(buildPlan func(int) physicalplan.PhysicalPlan) int {
	id := len(b.stages)
	return b.addStage(buildPlan(id))
}

func (b *stageBuilder) buildPlan(plan physicalplan.PhysicalPlan) (physicalplan.PhysicalPlan, error) {
	switch p := plan.(type) {
	case physicalplan.ScanExec:
		return p, nil
	case physicalplan.ProjectionExec:
		input, err := b.buildPlan(p.Input)
		if err != nil {
			return nil, err
		}
		p.Input = input
		return p, nil
	case physicalplan.HashAggregateExec:
		return b.buildAggregate(p)
	case physicalplan.HashJoinExec:
		if len(p.LeftKeys) != 1 || len(p.RightKeys) != 1 {
			return nil, fmt.Errorf("distributed join requires one key per input")
		}
		left, err := b.buildInput(p.Left, p.LeftKeys[0])
		if err != nil {
			return nil, err
		}
		right, err := b.buildInput(p.Right, p.RightKeys[0])
		if err != nil {
			return nil, err
		}
		p.Left, p.Right = left, right
		return p, nil
	default:
		return nil, fmt.Errorf("unsupported distributed physical plan: %T", plan)
	}
}

func (b *stageBuilder) buildInput(plan physicalplan.PhysicalPlan, keyIndex int) (physicalplan.PhysicalPlan, error) {
	schema := plan.Schema()
	if keyIndex < 0 || keyIndex >= len(schema.Fields) {
		return nil, fmt.Errorf("join key index %d out of range", keyIndex)
	}
	input, err := b.buildPlan(plan)
	if err != nil {
		return nil, err
	}
	id := b.addStageWithID(func(id int) physicalplan.PhysicalPlan {
		return ExchangeWriteExec{Input: input, StageID: id, Partitioning: HashPartitioning{Keys: []int{keyIndex}}}
	})
	return ExchangeReadExec{StageID: id, OutputSchema: schema, Partitioning: HashPartitioning{Keys: []int{keyIndex}}}, nil
}

func (b *stageBuilder) buildAggregate(aggregate physicalplan.HashAggregateExec) (physicalplan.PhysicalPlan, error) {
	if aggregate.Mode != physicalplan.AggregateComplete || len(aggregate.GroupExpr) > 1 {
		return nil, fmt.Errorf("distributed aggregation requires complete mode and at most one grouping expression")
	}
	input, err := b.buildPlan(aggregate.Input)
	if err != nil {
		return nil, err
	}
	partialSchema := datasource.Schema{Fields: append([]datasource.Field(nil), aggregate.OutputSchema.Fields[:len(aggregate.GroupExpr)]...)}
	finalExprs := make([]physicalplan.AggregateExpr, 0, len(aggregate.AggregateExpr))
	for _, expr := range aggregate.AggregateExpr {
		if _, ok := expr.(physicalplan.AvgExpr); !ok {
			return nil, fmt.Errorf("unsupported distributed aggregate: %T", expr)
		}
		index := len(partialSchema.Fields)
		partialSchema.Fields = append(partialSchema.Fields, datasource.Field{Name: "sum", Type: arrow.PrimitiveTypes.Float64}, datasource.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64})
		finalExprs = append(finalExprs, physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: index}, CountExpr: physicalplan.ColumnExpr{Index: index + 1}})
	}
	partial := physicalplan.HashAggregateExec{
		Input:         input,
		Mode:          physicalplan.AggregatePartial,
		OutputSchema:  partialSchema,
		GroupExpr:     aggregate.GroupExpr,
		AggregateExpr: aggregate.AggregateExpr,
	}
	var partitioning Partitioning = SinglePartition{}
	var finalGroups []physicalplan.PhysicalExpr
	if len(aggregate.GroupExpr) > 0 {
		partitioning = HashPartitioning{Keys: []int{0}}
		finalGroups = []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}}
	}
	id := b.addStageWithID(func(id int) physicalplan.PhysicalPlan {
		return ExchangeWriteExec{Input: partial, StageID: id, Partitioning: partitioning}
	})
	return physicalplan.HashAggregateExec{
		Input:         ExchangeReadExec{StageID: id, OutputSchema: partialSchema, Partitioning: partitioning},
		Mode:          physicalplan.AggregateFinal,
		OutputSchema:  aggregate.OutputSchema,
		GroupExpr:     finalGroups,
		AggregateExpr: finalExprs,
	}, nil
}
