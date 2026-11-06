package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/parser"
	"github.com/gabrielelanaro/queng/physicalplan"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "queng:", err)
		os.Exit(1)
	}
}

type tableFlags []string

func (t *tableFlags) String() string { return strings.Join(*t, ", ") }
func (t *tableFlags) Set(value string) error {
	*t = append(*t, value)
	return nil
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stderr, `Usage: queng "SELECT price FROM inventory" --table inventory=inventory.csv`)
		if len(args) == 0 {
			return fmt.Errorf("a SQL query and at least one --table are required")
		}
		return nil
	}
	query := args[0]
	flags := flag.NewFlagSet("queng", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var tableArgs tableFlags
	flags.Var(&tableArgs, "table", "register a CSV table as name=path (repeatable)")
	batchSize := flags.Int("batch-size", 1024, "maximum rows per input batch")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	if len(tableArgs) == 0 {
		return fmt.Errorf("at least one --table name=path is required")
	}
	statement, err := parser.Parse(query)
	if err != nil {
		return err
	}
	tables := make(map[string]datasource.DataSource, len(tableArgs))
	for _, arg := range tableArgs {
		name, path, ok := strings.Cut(arg, "=")
		if !ok || name == "" || path == "" {
			return fmt.Errorf("invalid table %q: expected name=path", arg)
		}
		if _, exists := tables[name]; exists {
			return fmt.Errorf("duplicate table %q", name)
		}
		source, err := datasource.NewCSVSource(path, *batchSize)
		if err != nil {
			return fmt.Errorf("table %q: %w", name, err)
		}
		tables[name] = source
	}
	logical, err := logicalplan.BuildLogicalPlan(statement, tables)
	if err != nil {
		return err
	}
	physical, err := physicalplan.BuildPhysicalPlan(logical)
	if err != nil {
		return err
	}
	schema := physical.Schema()
	header := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		header[i] = field.Name
	}
	var records [][]string
	for batch, err := range physical.Execute() {
		if err != nil {
			return err
		}
		rows := 0
		if batch.ColumnCount() > 0 {
			rows = batch.Column(0).Len()
		}
		for row := 0; row < rows; row++ {
			record := make([]string, batch.ColumnCount())
			for i := 0; i < batch.ColumnCount(); i++ {
				value := batch.Column(i).Value(row)
				if value == nil {
					record[i] = "NULL"
				} else {
					record[i] = fmt.Sprint(value)
				}
			}
			records = append(records, record)
		}
		batch.Release()
	}
	return writeTable(stdout, header, records)
}
