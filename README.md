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

## Run the distributed join fixture

With Docker and the Compose plugin installed, start one coordinator and two workers:

```sh
docker compose up --build -d
```

Submit a query to the coordinator:

```sh
curl -fsS -X POST http://localhost:8080/query \
  --data 'SELECT sales.product_id, sales.sale, products.product FROM sales LEFT JOIN products ON sales.product_id = products.product_id'
```

The response is JSON. It contains four rows: product 1 appears twice because it has two matches, product 2 appears once, and product 3 has a null product. Row order is unspecified.

```json
{"columns":["product_id","sale","product"],"rows":[[1,10,"apple"],[1,10,"pear"],[2,20,"book"],[3,30,null]]}
```

The coordinator listens on `localhost:8080` for queries. Workers communicate with it and each other over the Compose network. All three containers mount `samples/` at `/data`, so the CSV paths in serialized plans are accessible on either worker. This fixture uses static worker addresses and in-memory shuffle storage; it does not yet clean up exchange data between queries.

```sh
docker compose logs -f
docker compose down
```

The containers run on a single host, but have separate processes and memory. They exercise the scheduler, gRPC calls, and exchange; they do not require a distributed filesystem.
