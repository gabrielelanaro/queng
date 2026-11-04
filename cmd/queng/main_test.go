package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunQueriesCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.csv")
	data := "category,price\nfruit,4\nbooks,10\nfruit,8\nbooks,20\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, query, want string
	}{
		{"projection", "SELECT price FROM inventory", "+-------+\n| price |\n+-------+\n| 4     |\n| 10    |\n| 8     |\n| 20    |\n+-------+\n"},
		{"grouped average", "SELECT AVG(price) FROM inventory GROUP BY category", "+-----+\n| AVG |\n+-----+\n| 6   |\n| 15  |\n+-----+\n"},
		{"grouped average with category", "SELECT AVG(price), category FROM inventory GROUP BY category", "+-----+----------+\n| AVG | category |\n+-----+----------+\n| 6   | fruit    |\n| 15  | books    |\n+-----+----------+\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output, errors bytes.Buffer
			err := run([]string{tc.query, "--table", "inventory=" + path, "--batch-size", "2"}, &output, &errors)
			if err != nil {
				t.Fatalf("run() returned an error: %v; stderr: %s", err, errors.String())
			}
			if got := output.String(); got != tc.want {
				t.Errorf("output = %q; want %q", got, tc.want)
			}
		})
	}
}

func TestRunReportsMissingColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inventory.csv")
	if err := os.WriteFile(path, []byte("price\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	err := run([]string{"SELECT missing FROM inventory", "--table", "inventory=" + path}, &output, &errors)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("run() error = %v; want missing column", err)
	}
}
