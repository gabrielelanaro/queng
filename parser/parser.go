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
	Qualifier string
	ID        string
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
	Join       *SqlJoin
	GroupBy    []SqlExpr
}

// JoinType describes how a SQL join handles unmatched rows.
type JoinType string

const LeftJoin JoinType = "LEFT"

// SqlJoin describes an equality join with another table.
type SqlJoin struct {
	Type      JoinType
	TableName SqlIdentifier
	LeftKey   SqlIdentifier
	RightKey  SqlIdentifier
}

func (SqlSelect) statement() {}

// Parse parses a SELECT statement with an optional single-column GROUP BY.
func Parse(query string) (SqlStatement, error) {
	parts := strings.Fields(strings.NewReplacer("(", " ( ", ")", " ) ", ",", " , ", "=", " = ").Replace(query))
	if len(parts) < 4 || !strings.EqualFold(parts[0], "SELECT") {
		return nil, fmt.Errorf("invalid SELECT statement: %q", query)
	}
	var projections []SqlExpr
	next := 1
	for {
		if next >= len(parts) || strings.EqualFold(parts[next], "FROM") {
			return nil, fmt.Errorf("invalid SELECT expression: %q", query)
		}
		name, ok := parseIdentifier(parts[next])
		if !ok {
			return nil, fmt.Errorf("invalid SELECT expression: %q", query)
		}
		next++
		var expr SqlExpr = name
		if next < len(parts) && parts[next] == "(" {
			if len(parts) < next+3 || name.Qualifier != "" || parts[next+2] != ")" {
				return nil, fmt.Errorf("invalid function call: %q", query)
			}
			arg, ok := parseIdentifier(parts[next+1])
			if !ok {
				return nil, fmt.Errorf("invalid function call: %q", query)
			}
			expr = SqlFunction{ID: name.ID, Args: []SqlExpr{arg}}
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
	if next < len(parts) && strings.EqualFold(parts[next], "LEFT") {
		if len(parts) < next+7 || !strings.EqualFold(parts[next+1], "JOIN") ||
			!identifier(parts[next+2]) || !strings.EqualFold(parts[next+3], "ON") || parts[next+5] != "=" {
			return nil, fmt.Errorf("invalid LEFT JOIN clause: %q", query)
		}
		left, leftOK := parseIdentifier(parts[next+4])
		right, rightOK := parseIdentifier(parts[next+6])
		if !leftOK || !rightOK {
			return nil, fmt.Errorf("invalid LEFT JOIN keys: %q", query)
		}
		stmt.Join = &SqlJoin{Type: LeftJoin, TableName: SqlIdentifier{ID: parts[next+2]}, LeftKey: left, RightKey: right}
		next += 7
	}
	if next < len(parts) {
		if len(parts) != next+3 || !strings.EqualFold(parts[next], "GROUP") || !strings.EqualFold(parts[next+1], "BY") {
			return nil, fmt.Errorf("invalid GROUP BY clause: %q", query)
		}
		group, ok := parseIdentifier(parts[next+2])
		if !ok {
			return nil, fmt.Errorf("invalid GROUP BY clause: %q", query)
		}
		stmt.GroupBy = []SqlExpr{group}
	}
	return stmt, nil
}

func parseIdentifier(s string) (SqlIdentifier, bool) {
	qualifier, name, qualified := strings.Cut(s, ".")
	if !qualified {
		return SqlIdentifier{ID: s}, identifier(s)
	}
	if !identifier(qualifier) || !identifier(name) {
		return SqlIdentifier{}, false
	}
	return SqlIdentifier{Qualifier: qualifier, ID: name}, true
}

func identifier(s string) bool {
	for i, r := range s {
		if r != '_' && !unicode.IsLetter(r) && (i == 0 || !unicode.IsDigit(r)) {
			return false
		}
	}
	return s != ""
}
