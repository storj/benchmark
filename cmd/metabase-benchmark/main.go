// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
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

	jsonout := flag.String("json", "measurement.json", "json benchmark output")

	flag.Parse()

	measurements, err := bench.Run(ctx, log)
	if err != nil {
		log.Fatal("Benchmark failed.", zap.Error(err))
	}

	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
	fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
		"Parts", "Segments", "",
		"Avg",
		"Max",
		"P50", "P90", "P99",
	)
	fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
		"", "", "",
		"ms",
		"ms",
		"ms", "ms", "ms",
	)
	for _, m := range measurements {
		m.PrintStats(w)
	}
	_ = w.Flush()

	if *jsonout != "" {
		data, err := json.Marshal(measurements)
		if err != nil {
			log.Fatal("JSON marshal failed", zap.Error(err))
		}
		_ = ioutil.WriteFile(*jsonout, data, 0644)
	}

	// if *plotname != "" {
	// 	err := Plot(*plotname, measurements)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
}
