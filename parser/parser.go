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

// SqlSelect represents a SELECT statement.
type SqlSelect struct {
	Projection []SqlExpr
	TableName  SqlIdentifier
	GroupBy    []SqlExpr
}

func (SqlSelect) statement() {}

// Parse parses a SELECT statement with an optional single-column GROUP BY.
func Parse(query string) (SqlStatement, error) {
	parts := strings.Fields(query)
	if (len(parts) != 4 && len(parts) != 7) || !strings.EqualFold(parts[0], "SELECT") ||
		!strings.EqualFold(parts[2], "FROM") || !identifier(parts[1]) || !identifier(parts[3]) {
		return nil, fmt.Errorf("invalid SELECT statement: %q", query)
	}
	stmt := SqlSelect{
		Projection: []SqlExpr{SqlIdentifier{ID: parts[1]}},
		TableName:  SqlIdentifier{ID: parts[3]},
	}
	if len(parts) == 7 {
		if !strings.EqualFold(parts[4], "GROUP") || !strings.EqualFold(parts[5], "BY") || !identifier(parts[6]) {
			return nil, fmt.Errorf("invalid GROUP BY clause: %q", query)
		}
		stmt.GroupBy = []SqlExpr{SqlIdentifier{ID: parts[6]}}
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
