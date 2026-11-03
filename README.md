# queng

A query engine written in Go to learn how query engines work, one piece at a time.

This repository starts with a runnable CLI. Query execution is the next step.

## Get started

Requires Go 1.27 or later.

```sh
go run ./cmd/queng
go test ./...
go build -o bin/queng ./cmd/queng
```

## Learning roadmap

Build and explain each layer with small examples:

1. Represent rows and values, then scan an in-memory table.
2. Add filter and projection operators.
3. Tokenize and parse a small SQL subset (`SELECT`, `FROM`, `WHERE`).
4. Turn parsed queries into execution plans.
5. Add sorting, limits, and aggregation.
6. Explore joins and simple query optimization.
7. Read tables from disk and compare execution strategies.

Keep the first version single-process and in-memory so the execution model is easy to inspect.

## Layout

- `cmd/queng/`: command-line entry point.
- `.github/workflows/ci.yml`: formatting, vet, tests, and build checks.

Engine packages will be introduced as we implement the corresponding concepts.
