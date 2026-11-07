// Package optimizer rewrites logical plans before physical planning.
package optimizer

import "github.com/gabrielelanaro/queng/logicalplan"

// OptimizerRule transforms a logical plan while preserving the query's result.
type OptimizerRule interface {
	Optimize(logicalplan.LogicalPlan) (logicalplan.LogicalPlan, error)
}

// Optimizer applies its rules in order, passing each result to the next rule.
type Optimizer struct {
	rules []OptimizerRule
}

func New(rules ...OptimizerRule) *Optimizer {
	return &Optimizer{rules: rules}
}

func (o *Optimizer) Optimize(plan logicalplan.LogicalPlan) (logicalplan.LogicalPlan, error) {
	for _, rule := range o.rules {
		optimized, err := rule.Optimize(plan)
		if err != nil {
			return nil, err
		}
		plan = optimized
	}
	return plan, nil
}
