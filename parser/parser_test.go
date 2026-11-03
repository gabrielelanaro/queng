package parser_test

import (
	"reflect"
	"testing"

	"github.com/gabrielelanaro/queng/parser"
)

func TestParseSelect(t *testing.T) {
	got, err := parser.Parse("SELECT price FROM inventory")
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}

	want := parser.SqlSelect{
		Projection: []parser.SqlExpr{
			parser.SqlIdentifier{ID: "price"},
		},
		TableName: parser.SqlIdentifier{ID: "inventory"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("Parse() = %#v; want %#v", got, want)
	}
}
