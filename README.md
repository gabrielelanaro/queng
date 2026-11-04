# queng

A query engine written in Go to learn how query engines work, one piece at a time.

## Run a query

Requires Go 1.27 or later. Register a CSV file under a table name and pass a SQL query:

```sh
go run ./cmd/queng "SELECT price FROM inventory" --table inventory=samples/inventory.csv
```

The first CSV row supplies column names. Queng infers integer, floating-point, and string column types by reading the file; empty fields are treated as nulls. Results are displayed as a text table.

The current SQL subset supports selected columns and `AVG(column)`, with an optional single-column `GROUP BY` for aggregate queries:

```sh
go run ./cmd/queng "SELECT AVG(price) FROM inventory GROUP BY category" --table inventory=samples/inventory.csv
```

The table option can be repeated to register more files. Use `--batch-size 100` to change the maximum number of rows read per batch (the default is 1024). SQL support is still limited: aliases, filters, multiple grouping columns, and trailing semicolons are not yet supported.

```sh
go test ./...
go build -o bin/queng ./cmd/queng
```

## Layout

- `cmd/queng/`: command-line entry point.
- `parser/`: SQL parser and syntax tree.
- `logicalplan/`: logical query planning.
- `physicalplan/`: physical planning and execution.
- `datasource/`: schemas, column vectors, and CSV input.
