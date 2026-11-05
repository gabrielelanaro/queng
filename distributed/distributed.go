// Package distributed plans and executes distributed queries.
package distributed

import (
	"fmt"
	"iter"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/physicalplan"
)

// Stage describes one part of a distributed physical plan.
type Stage struct {
	ID   int
	Plan physicalplan.PhysicalPlan
}

// ExchangeWrite partitions its input for downstream stages.
type ExchangeWrite struct {
	Input    physicalplan.PhysicalPlan
	KeyIndex int
}

func (e ExchangeWrite) Schema() datasource.Schema { return e.Input.Schema() }

func (e ExchangeWrite) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{e.Input}
}

func (e ExchangeWrite) Execute() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		yield(datasource.RecordBatch{}, fmt.Errorf("exchange write execution is not yet implemented"))
	}
}

// ExchangeRead reads the partition produced by an upstream stage.
type ExchangeRead struct {
	StageID      int
	OutputSchema datasource.Schema
}

func (e ExchangeRead) Schema() datasource.Schema { return e.OutputSchema }

func (ExchangeRead) Children() []physicalplan.PhysicalPlan { return nil }

func (e ExchangeRead) Execute() iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		yield(datasource.RecordBatch{}, fmt.Errorf("exchange read execution is not yet implemented"))
	}
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
	b.stages = append(b.stages, Stage{ID: id, Plan: plan})
	return id
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
	id := b.addStage(ExchangeWrite{Input: input, KeyIndex: keyIndex})
	return ExchangeRead{StageID: id, OutputSchema: schema}, nil
}
