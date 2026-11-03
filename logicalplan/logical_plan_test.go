package logicalplan_test

import (
	"reflect"
	"testing"

	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/parser"
)

func TestBuildLogicalPlan(t *testing.T) {
	query := parser.SqlSelect{
		Projection: []parser.SqlExpr{
			parser.SqlIdentifier{ID: "price"},
		},
		TableName: parser.SqlIdentifier{ID: "inventory"},
	}

	got, err := logicalplan.BuildLogicalPlan(query)
	if err != nil {
		t.Fatalf("BuildLogicalPlan() returned an error: %v", err)
	}

	want := logicalplan.Projection{
		Input: logicalplan.Scan{TableName: "inventory"},
		Expr: []logicalplan.LogicalExpr{
			logicalplan.Column{Name: "price"},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("BuildLogicalPlan() = %#v; want %#v", got, want)
	}
}
