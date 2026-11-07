package planserde_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/distributed"
	"github.com/gabrielelanaro/queng/execution"
	"github.com/gabrielelanaro/queng/logicalplan"
	"github.com/gabrielelanaro/queng/physicalplan"
	"github.com/gabrielelanaro/queng/planserde"
	"google.golang.org/protobuf/proto"
)

func TestRoundTripProjectionOverCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.csv")
	if err := os.WriteFile(path, []byte("quantity,price\n2,9.5\n3,12.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewCSVSource(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	schema := datasource.Schema{Fields: []datasource.Field{{Qualifier: "inventory", Name: "price", Type: arrow.PrimitiveTypes.Float64}}}
	plan := physicalplan.ProjectionExec{
		Input: physicalplan.ScanExec{Source: source}, OutputSchema: schema,
		Expr: []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 1}},
	}
	message, err := planserde.SerializePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(planserde.Plan)
	if err := proto.Unmarshal(data, decoded); err != nil {
		t.Fatal(err)
	}
	got, err := planserde.DeserializePlan(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Schema(), schema) {
		t.Fatalf("schema = %#v; want %#v", got.Schema(), schema)
	}
	var values []any
	var batches int
	for batch, err := range got.Execute(execution.Context{}, 0) {
		if err != nil {
			t.Fatal(err)
		}
		batches++
		for i := range batch.Column(0).Len() {
			values = append(values, batch.Column(0).Value(i))
		}
		batch.Release()
	}
	if !reflect.DeepEqual(values, []any{9.5, 12.0}) || batches != 2 {
		t.Errorf("got %v in %d batches; want [9.5 12] in 2", values, batches)
	}
}

func TestRoundTripJoinStages(t *testing.T) {
	dir := t.TempDir()
	salesPath := filepath.Join(dir, "sales.csv")
	productsPath := filepath.Join(dir, "products.csv")
	if err := os.WriteFile(salesPath, []byte("product_id,sale\n1,10\n2,20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(productsPath, []byte("product_id,product\n1,apple\n"), 0600); err != nil {
		t.Fatal(err)
	}
	sales, err := datasource.NewCSVSource(salesPath, 10)
	if err != nil {
		t.Fatal(err)
	}
	products, err := datasource.NewCSVSource(productsPath, 10)
	if err != nil {
		t.Fatal(err)
	}
	schema := datasource.Schema{Fields: append(append([]datasource.Field{}, sales.Schema().Fields...), products.Schema().Fields...)}
	plan := physicalplan.HashJoinExec{Left: physicalplan.ScanExec{Source: sales}, Right: physicalplan.ScanExec{Source: products}, Type: logicalplan.LeftJoin, LeftKeys: []int{0}, RightKeys: []int{0}, OutputSchema: schema}
	stages, err := distributed.BuildStages(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range stages {
		msg, err := planserde.SerializePlan(stage.Plan)
		if err != nil {
			t.Fatal(err)
		}
		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		msg = new(planserde.Plan)
		if err := proto.Unmarshal(data, msg); err != nil {
			t.Fatal(err)
		}
		decoded, err := planserde.DeserializePlan(msg)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded, stage.Plan) {
			t.Errorf("stage %d = %#v; want %#v", stage.ID, decoded, stage.Plan)
		}
	}
}

func TestRoundTripGroupedAverage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.csv")
	if err := os.WriteFile(path, []byte("category,price\nfruit,10\nfruit,20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewCSVSource(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	schema := datasource.Schema{Fields: []datasource.Field{{Name: "category", Type: arrow.BinaryTypes.String}, {Name: "AVG", Type: arrow.PrimitiveTypes.Float64}}}
	plan := physicalplan.HashAggregateExec{Input: physicalplan.ScanExec{Source: source}, OutputSchema: schema,
		GroupExpr:     []physicalplan.PhysicalExpr{physicalplan.ColumnExpr{Index: 0}},
		AggregateExpr: []physicalplan.AggregateExpr{physicalplan.AvgExpr{Expr: physicalplan.ColumnExpr{Index: 1}}},
	}
	msg, err := planserde.SerializePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	msg = new(planserde.Plan)
	if err := proto.Unmarshal(data, msg); err != nil {
		t.Fatal(err)
	}
	got, err := planserde.DeserializePlan(msg)
	if err != nil {
		t.Fatal(err)
	}
	var values []any
	for batch, err := range got.Execute(execution.Context{}, 0) {
		if err != nil {
			t.Fatal(err)
		}
		for col := range batch.ColumnCount() {
			values = append(values, batch.Column(col).Value(0))
		}
		batch.Release()
	}
	if !reflect.DeepEqual(values, []any{"fruit", float64(15)}) {
		t.Errorf("values = %v; want [fruit 15]", values)
	}
}

func TestRoundTripParquetScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "price", Type: arrow.PrimitiveTypes.Int64}}, nil)
	writer, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`[{"price":10}]`, `[{"price":20}]`} {
		record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		err = writer.Write(record)
		record.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := datasource.NewParquetSource(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := planserde.SerializePlan(physicalplan.ScanExec{Source: source})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var message planserde.Plan
	if err := proto.Unmarshal(wire, &message); err != nil {
		t.Fatal(err)
	}
	decoded, err := planserde.DeserializePlan(&message)
	if err != nil {
		t.Fatal(err)
	}
	scan := decoded.(physicalplan.ScanExec)
	if scan.Source.PartitionCount() != 2 {
		t.Fatalf("partition count = %d; want 2", scan.Source.PartitionCount())
	}
	var values []any
	for batch, err := range scan.Source.Scan(1) {
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, batch.Column(0).Value(0))
		batch.Release()
	}
	if !reflect.DeepEqual(values, []any{int64(20)}) {
		t.Errorf("partition 1 = %v; want [20]", values)
	}
}
