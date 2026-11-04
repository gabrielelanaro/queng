// Package parser parses a small subset of SQL.
package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// SqlStatement is a SQL statement.
type SqlStatement interface {
	statement()
}

// SqlExpr is an expression in a SQL statement.
type SqlExpr interface {
	sqlExpr()
}

// SqlIdentifier is a SQL name.
type SqlIdentifier struct {
	ID string
}

func (SqlIdentifier) sqlExpr() {}

// SqlFunction is a SQL function call.
type SqlFunction struct {
	ID   string
	Args []SqlExpr
}

func (SqlFunction) sqlExpr() {}

// SqlSelect represents a SELECT statement.
type SqlSelect struct {
	Projection []SqlExpr
	TableName  SqlIdentifier
	GroupBy    []SqlExpr
}

func (SqlSelect) statement() {}

// Parse parses a SELECT statement with an optional single-column GROUP BY.
func Parse(query string) (SqlStatement, error) {
	parts := strings.Fields(strings.NewReplacer("(", " ( ", ")", " ) ", ",", " , ").Replace(query))
	if len(parts) < 4 || !strings.EqualFold(parts[0], "SELECT") {
		return nil, fmt.Errorf("invalid SELECT statement: %q", query)
	}
	var projections []SqlExpr
	next := 1
	for {
		if next >= len(parts) || !identifier(parts[next]) || strings.EqualFold(parts[next], "FROM") {
			return nil, fmt.Errorf("invalid SELECT expression: %q", query)
		}
		name := parts[next]
		next++
		var expr SqlExpr = SqlIdentifier{ID: name}
		if next < len(parts) && parts[next] == "(" {
			if len(parts) < next+3 || !identifier(parts[next+1]) || parts[next+2] != ")" {
				return nil, fmt.Errorf("invalid function call: %q", query)
			}
			expr = SqlFunction{ID: name, Args: []SqlExpr{SqlIdentifier{ID: parts[next+1]}}}
			next += 3
		}
		projections = append(projections, expr)
		if next >= len(parts) || parts[next] != "," {
			break
		}
		next++
	}
	if len(parts) < next+2 || !strings.EqualFold(parts[next], "FROM") || !identifier(parts[next+1]) {
		return nil, fmt.Errorf("invalid SELECT statement: %q", query)
	}
	stmt := SqlSelect{
		Projection: projections,
		TableName:  SqlIdentifier{ID: parts[next+1]},
	}
	next += 2
	if next < len(parts) {
		if len(parts) != next+3 || !strings.EqualFold(parts[next], "GROUP") || !strings.EqualFold(parts[next+1], "BY") || !identifier(parts[next+2]) {
			return nil, fmt.Errorf("invalid GROUP BY clause: %q", query)
		}
		stmt.GroupBy = []SqlExpr{SqlIdentifier{ID: parts[next+2]}}
	}
	return stmt, nil
}

func identifier(s string) bool {
	for i, r := range s {
		if r != '_' && !unicode.IsLetter(r) && (i == 0 || !unicode.IsDigit(r)) {
			return false
		}
	}
	return s != ""
}
