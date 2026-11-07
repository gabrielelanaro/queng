// Package distributed plans and executes distributed queries.
package distributed

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"iter"
	"math"

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

// ExchangeWrite partitions its input for downstream stages.
type ExchangeWrite struct {
	Input    physicalplan.PhysicalPlan
	StageID  int
	KeyIndex int
}

func (e ExchangeWrite) Schema() datasource.Schema { return e.Input.Schema() }

func (e ExchangeWrite) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{e.Input}
}

func (e ExchangeWrite) Execute(ctx execution.Context, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		if ctx.Exchange == nil {
			yield(datasource.RecordBatch{}, fmt.Errorf("exchange store is not configured"))
			return
		}
		for batch, err := range e.Input.Execute(ctx, partitionID) {
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
			err = e.writeBatch(ctx, batch)
			batch.Release()
			if err != nil {
				yield(datasource.RecordBatch{}, err)
				return
			}
		}
	}
}

// writeBatch groups rows by output partition and writes each group as a batch.
func (e ExchangeWrite) writeBatch(ctx execution.Context, batch datasource.RecordBatch) error {
	if e.KeyIndex < 0 || e.KeyIndex >= batch.ColumnCount() {
		return fmt.Errorf("exchange key index %d out of range", e.KeyIndex)
	}
	rowsByPartition := make([][]int, ctx.ShufflePartitions)
	keyColumn := batch.Column(e.KeyIndex)
	for row := range keyColumn.Len() {
		hash, err := hashKey(keyColumn.Value(row))
		if err != nil {
			return err
		}
		partition := int(hash % uint64(ctx.ShufflePartitions))
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
		key := execution.ExchangeKey{JobID: ctx.JobID, StageID: e.StageID, TaskID: ctx.TaskID, PartitionID: partitionID}
		err = ctx.Exchange.Write(key, output)
		output.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// hashKey uses a stable hash so equal keys route identically on every worker.
func hashKey(value any) (uint64, error) {
	h := fnv.New64a()
	var bits [8]byte
	switch v := value.(type) {
	case nil:
		h.Write([]byte{0})
	case int64:
		h.Write([]byte{1})
		binary.LittleEndian.PutUint64(bits[:], uint64(v))
		h.Write(bits[:])
	case float64:
		h.Write([]byte{2})
		if v == 0 {
			v = 0 // Both signs of zero compare equal.
		}
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(v))
		h.Write(bits[:])
	case string:
		h.Write([]byte{3})
		h.Write([]byte(v))
	case bool:
		h.Write([]byte{4})
		if v {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	default:
		return 0, fmt.Errorf("unsupported exchange key type: %T", value)
	}
	return h.Sum64(), nil
}

// ExchangeRead reads the partition produced by an upstream stage.
type ExchangeRead struct {
	StageID      int
	OutputSchema datasource.Schema
}

func (e ExchangeRead) Schema() datasource.Schema { return e.OutputSchema }

func (ExchangeRead) Children() []physicalplan.PhysicalPlan { return nil }

func (e ExchangeRead) Execute(ctx execution.Context, partitionID int) iter.Seq2[datasource.RecordBatch, error] {
	if ctx.Exchange == nil {
		return func(yield func(datasource.RecordBatch, error) bool) {
			yield(datasource.RecordBatch{}, fmt.Errorf("exchange store is not configured"))
		}
	}
	return ctx.Exchange.ReadPartition(ctx.Context, ctx.JobID, e.StageID, partitionID)
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
	if read, ok := plan.(ExchangeRead); ok {
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
		return ExchangeWrite{Input: input, StageID: id, KeyIndex: keyIndex}
	})
	return ExchangeRead{StageID: id, OutputSchema: schema}, nil
}
