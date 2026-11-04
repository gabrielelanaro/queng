package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// writeTable formats query results after measuring the width of each column.
func writeTable(output io.Writer, header []string, rows [][]string) error {
	widths := make([]int, len(header))
	for i := range header {
		header[i] = tableCell(header[i])
		widths[i] = utf8.RuneCountInString(header[i])
	}
	for _, row := range rows {
		for i := range row {
			row[i] = tableCell(row[i])
			if width := utf8.RuneCountInString(row[i]); width > widths[i] {
				widths[i] = width
			}
		}
	}
	writer := bufio.NewWriter(output)
	border := func() {
		fmt.Fprint(writer, "+")
		for _, width := range widths {
			fmt.Fprint(writer, strings.Repeat("-", width+2), "+")
		}
		fmt.Fprintln(writer)
	}
	writeRow := func(row []string) {
		fmt.Fprint(writer, "|")
		for i, value := range row {
			padding := widths[i] - utf8.RuneCountInString(value)
			fmt.Fprint(writer, " ", value, strings.Repeat(" ", padding), " |")
		}
		fmt.Fprintln(writer)
	}
	border()
	writeRow(header)
	border()
	for _, row := range rows {
		writeRow(row)
	}
	border()
	return writer.Flush()
}

func tableCell(value string) string {
	return strings.NewReplacer("\n", `\n`, "\r", `\r`, "\t", `\t`).Replace(value)
}
