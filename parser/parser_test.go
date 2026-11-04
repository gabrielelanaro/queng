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

func TestParseSelectWithGroupBy(t *testing.T) {
	got, err := parser.Parse("SELECT category FROM inventory GROUP BY category")
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}

	want := parser.SqlSelect{
		Projection: []parser.SqlExpr{parser.SqlIdentifier{ID: "category"}},
		TableName:  parser.SqlIdentifier{ID: "inventory"},
		GroupBy:    []parser.SqlExpr{parser.SqlIdentifier{ID: "category"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Parse() = %#v; want %#v", got, want)
	}
}

func TestParseSelectFunction(t *testing.T) {
	got, err := parser.Parse("SELECT AVG(price) FROM inventory")
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}

	want := parser.SqlSelect{
		Projection: []parser.SqlExpr{
			parser.SqlFunction{
				ID: "AVG",
				Args: []parser.SqlExpr{
					parser.SqlIdentifier{ID: "price"},
				},
			},
		},
		TableName: parser.SqlIdentifier{ID: "inventory"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Parse() = %#v; want %#v", got, want)
	}
}

func TestParseSelectMultipleExpressions(t *testing.T) {
	got, err := parser.Parse("SELECT AVG(price), category FROM inventory GROUP BY category")
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}
	want := parser.SqlSelect{
		Projection: []parser.SqlExpr{
			parser.SqlFunction{ID: "AVG", Args: []parser.SqlExpr{parser.SqlIdentifier{ID: "price"}}},
			parser.SqlIdentifier{ID: "category"},
		},
		TableName: parser.SqlIdentifier{ID: "inventory"},
		GroupBy:   []parser.SqlExpr{parser.SqlIdentifier{ID: "category"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Parse() = %#v; want %#v", got, want)
	}
}

func TestParseSelectWithLeftJoin(t *testing.T) {
	got, err := parser.Parse("SELECT sales.product_id FROM sales LEFT JOIN products ON sales.product_id = products.product_id")
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}

	want := parser.SqlSelect{
		Projection: []parser.SqlExpr{
			parser.SqlIdentifier{Qualifier: "sales", ID: "product_id"},
		},
		TableName: parser.SqlIdentifier{ID: "sales"},
		Join: &parser.SqlJoin{
			Type:      parser.LeftJoin,
			TableName: parser.SqlIdentifier{ID: "products"},
			LeftKey:   parser.SqlIdentifier{Qualifier: "sales", ID: "product_id"},
			RightKey:  parser.SqlIdentifier{Qualifier: "products", ID: "product_id"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Parse() = %#v; want %#v", got, want)
	}
}

func TestParseRejectsIncompleteJoinCondition(t *testing.T) {
	_, err := parser.Parse("SELECT price FROM sales LEFT JOIN products ON sales.id =")
	if err == nil {
		t.Fatal("Parse() succeeded with an incomplete join condition")
	}
}
