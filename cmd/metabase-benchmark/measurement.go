// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"fmt"
	"io"
	"time"

	"github.com/loov/hrtime"
)

// Measurement contains measurements for different requests.
type Measurement struct {
	Scenario
	Results []*Result
}

// Result contains durations for specific tests.
type Result struct {
	Name      string
	Durations []time.Duration
}

// Result finds or creates a result with the specified name.
func (m *Measurement) Result(name string) *Result {
	for _, x := range m.Results {
		if x.Name == name {
			return x
		}
	}

	r := &Result{}
	r.Name = name
	m.Results = append(m.Results, r)
	return r
}

// Record records a time measurement.
func (m *Measurement) Record(name string, duration time.Duration) {
	r := m.Result(name)
	r.Durations = append(r.Durations, duration)
}

// PrintStats prints important valueas about the measurement.
func (m *Measurement) PrintStats(w io.Writer) {
	type Hist struct {
		*Result
		*hrtime.Histogram
	}

	hists := []Hist{}
	for _, result := range m.Results {
		hists = append(hists, Hist{
			Result: result,
			Histogram: hrtime.NewDurationHistogram(result.Durations, &hrtime.HistogramOptions{
				BinCount:        10,
				NiceRange:       true,
				ClampMaximum:    0,
				ClampPercentile: 0.999,
			}),
		})
	}

	msec := func(ns float64) string {
		return fmt.Sprintf("%.2f", ns/1e6)
	}

	for _, hist := range hists {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			m.Parts, m.Segments, hist.Name,
			msec(hist.Average),
			msec(hist.Maximum),
			msec(hist.P50),
			msec(hist.P90),
			msec(hist.P99),
		)
	}
}
