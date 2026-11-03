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
}

func (SqlSelect) statement() {}

// Parse parses a SELECT statement with one identifier and one table name.
func Parse(query string) (SqlStatement, error) {
	parts := strings.Fields(query)
	if len(parts) != 4 || !strings.EqualFold(parts[0], "SELECT") ||
		!strings.EqualFold(parts[2], "FROM") || !identifier(parts[1]) || !identifier(parts[3]) {
		return nil, fmt.Errorf("invalid SELECT statement: %q", query)
	}
	return SqlSelect{
		Projection: []SqlExpr{SqlIdentifier{ID: parts[1]}},
		TableName:  SqlIdentifier{ID: parts[3]},
	}, nil
}

func identifier(s string) bool {
	for i, r := range s {
		if r != '_' && !unicode.IsLetter(r) && (i == 0 || !unicode.IsDigit(r)) {
			return false
		}
	}
	return s != ""
}
