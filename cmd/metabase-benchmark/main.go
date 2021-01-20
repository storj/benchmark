// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"text/tabwriter"

	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to created logger: %v\n", err)
		os.Exit(1)
	}

	bench := NewBenchmark("postgres://postgres@localhost/benchmark?sslmode=disable")

	flag.StringVar(&bench.DBURL, "database-url", bench.DBURL, "database url")
	flag.IntVar(&bench.Count, "count", bench.Count, "benchmark count")
	flag.DurationVar(&bench.MaxDuration, "time", bench.MaxDuration, "maximum benchmark time per scenario")

	load := flag.String("load", "", "load measurements from json")

	type Output struct {
		Type string
		File string
	}
	first := true
	outputs := []Output{{"table", ""}}

	flag.Var(funcFlag(func(out string) error {
		if first {
			outputs = []Output{}
			first = false
		}
		tokens := strings.SplitN(out, ":", 2)
		if len(tokens) == 2 {
			outputs = append(outputs, Output{Type: tokens[0], File: tokens[1]})
		} else {
			outputs = append(outputs, Output{Type: out})
		}
		return nil
	}), "out", "type:file, supported types (table, std, json, svg)")

	flag.Parse()

	var measurements []Measurement

	if *load != "" {
		data, err := ioutil.ReadFile(*load)
		if err != nil {
			log.Fatal("Benchmark failed.", zap.Error(err))
		}
		err = json.Unmarshal(data, &measurements)
		if err != nil {
			log.Fatal("Benchmark failed.", zap.Error(err))
		}
	} else {
		measurements, err = bench.Run(ctx, log)
		if err != nil {
			log.Fatal("Benchmark failed.", zap.Error(err))
		}
	}

	fmt.Println()
	for _, out := range outputs {
		func(out Output) {
			var output io.Writer
			output = os.Stdout
			if out.File != "" {
				f, err := os.Create(out.File)
				if err != nil {
					log.Error("failed to open file, writing to stdout", zap.String("file", out.File))
				} else {
					defer func() { _ = f.Close() }()
					output = f
				}
			}

			switch out.Type {
			case "table":
				err := WriteTable(ctx, output, measurements)
				if err != nil {
					log.Error("writing table failed", zap.Error(err))
				}
			case "std":
				err := WriteBenchStat(ctx, output, measurements)
				if err != nil {
					log.Error("writing benchstat failed", zap.Error(err))
				}
			case "json":
				err := json.NewEncoder(output).Encode(measurements)
				if err != nil {
					log.Error("writing json failed", zap.Error(err))
				}
			default:
				log.Error("output type not supported", zap.String("type", out.Type), zap.String("file", out.File))
			}
		}(out)
	}
}

// WriteTable writes measurements as a formatted table to w.
func WriteTable(ctx context.Context, w io.Writer, measurements []Measurement) error {
	tw := tabwriter.NewWriter(w, 0, 0, 4, ' ', 0)
	defer func() { _ = tw.Flush() }()

	fmt.Fprintf(tw, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
		"Parts", "Segments", "",
		"Avg",
		"Max",
		"P50", "P90", "P99",
	)
	fmt.Fprintf(tw, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
		"", "", "",
		"ms",
		"ms",
		"ms", "ms", "ms",
	)
	for _, m := range measurements {
		m.PrintStats(tw)
	}

	return nil
}

// WriteBenchStat writes measurements such that they are compatible with benchstat.
func WriteBenchStat(ctx context.Context, w io.Writer, measurements []Measurement) error {
	return nil
}

// funcFlag is an implementation of Go 1.16 flag.Func.
type funcFlag func(string) error

func (f funcFlag) Set(s string) error { return f(s) }
func (f funcFlag) String() string     { return "" }
