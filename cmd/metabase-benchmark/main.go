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
	"path/filepath"
	"regexp"
	"strings"
	"text/tabwriter"

	"github.com/loov/hrtime"
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

	var loads []string
	flag.Var(funcFlag(func(out string) error {
		loads = append(loads, out)
		return nil
	}), "load", "load measurements from json")

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
	}), "out", "type:file, supported types (table, std, json, plot-percentile)")

	flag.Parse()

	var results []BenchmarkResult

	if len(loads) > 0 {
		for _, name := range loads {
			data, err := ioutil.ReadFile(name)
			if err != nil {
				log.Fatal("Benchmark failed.", zap.Error(err))
			}
			var measurements []Measurement
			err = json.Unmarshal(data, &measurements)
			if err != nil {
				log.Fatal("Benchmark failed.", zap.Error(err))
			}
			results = append(results, BenchmarkResult{
				Name:         filepath.Base(strings.TrimSuffix(name, filepath.Ext(name))),
				Measurements: measurements,
			})
		}
	} else {
		measurements, err := bench.Run(ctx, log)
		if err != nil {
			log.Fatal("Benchmark failed.", zap.Error(err))
		}
		results = append(results, BenchmarkResult{
			Name:         "Benchmark",
			Measurements: measurements,
		})
	}

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

			if output == os.Stdout {
				fmt.Println()
			}

			switch out.Type {
			case "table":
				if len(results) != 1 {
					log.Error("multiple measurments not supported for 'table'")
					return
				}

				err := WriteTable(ctx, output, results[0].Measurements)
				if err != nil {
					log.Error("writing table failed", zap.Error(err))
				}
			case "std":
				if len(results) != 1 {
					log.Error("multiple measurments not supported for 'std'")
					return
				}

				err := WriteBenchStat(ctx, output, results[0].Measurements)
				if err != nil {
					log.Error("writing benchstat failed", zap.Error(err))
				}

			case "json":
				if len(results) != 1 {
					log.Error("multiple measurments not supported for 'json'")
					return
				}

				err := json.NewEncoder(output).Encode(results[0].Measurements)
				if err != nil {
					log.Error("writing json failed", zap.Error(err))
				}

			case "plot-percentile":
				err := PlotPercentiles(ctx, output, results)
				if err != nil {
					log.Error("writing plot failed", zap.Error(err))
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

var rxSpace = regexp.MustCompile(`\s+`)

// WriteBenchStat writes measurements such that they are compatible with benchstat.
//
// Specification https://go.googlesource.com/proposal/+/master/design/14313-benchmark-format.md.
func WriteBenchStat(ctx context.Context, w io.Writer, measurements []Measurement) error {
	for _, m := range measurements {
		for _, r := range m.Results {
			test := rxSpace.ReplaceAllString(r.Name, "")
			name := fmt.Sprintf("Benchmark%s/parts=%d/segments=%d", test, m.Parts, m.Segments)

			h := hrtime.NewDurationHistogram(r.Durations, &hrtime.HistogramOptions{
				BinCount:        10,
				NiceRange:       true,
				ClampMaximum:    0,
				ClampPercentile: 0.999,
			})
			fmt.Fprintf(w, "%s  %10d  %10.0f ns/op  %10.0f ns/p90  %10.0f ns/p99\n", name, len(r.Durations), h.Average, h.P90, h.P99)
		}
	}
	return nil
}

// funcFlag is an implementation of Go 1.16 flag.Func.
type funcFlag func(string) error

func (f funcFlag) Set(s string) error { return f(s) }
func (f funcFlag) String() string     { return "" }
