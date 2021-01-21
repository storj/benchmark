// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"image/color"
	"io"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/loov/plot"
	"github.com/loov/plot/plotsvg"
)

// PlotPercentiles percentiles on top of each other.
func PlotPercentiles(ctx context.Context, w io.Writer, results []BenchmarkResult) error {
	p := plot.New()
	const pad = 5

	rowStack := plot.NewVFlex()
	rowStack.Margin = plot.R(pad, pad, pad, pad)
	p.Add(rowStack)

	sections := []string{}
	for _, br := range results {
		// collect the measurements and result names in reverse order
		// because the first runs don't have all the entries needed.
		for i := len(br.Measurements) - 1; i >= 0; i-- {
			m := &br.Measurements[i]
			for i := len(m.Results) - 1; i >= 0; i-- {
				r := m.Results[i]
				includeString(&sections, r.Name)
			}
		}
	}
	reverseStrings(sections)

	partVariants := []int{}
	segmentVariants := []int{}
	for _, br := range results {
		for _, m := range br.Measurements {
			// ignore IterateObjects, since it doesn't have a "Scenario".
			if m.Parts == 0 && m.Segments == 0 {
				continue
			}
			includeInt(&partVariants, m.Parts)
			includeInt(&segmentVariants, m.Segments)
		}
	}

	columns := len(segmentVariants)
	totalHeight := plot.Length(0.0)

	const captionHeight = 20 + pad*2
	const gridCellHeight = 150
	const gridCellWidth = 200

	benchmarks := &plot.HStack{}
	for bri, br := range results {
		text := plot.NewTextbox(br.Name)
		text.Size = 15
		text.Fill = palette[bri%len(palette)]
		text.Class = "bold"
		benchmarks.Add(text)
	}
	rowStack.Add(25+pad*2, benchmarks)
	totalHeight += 25 + pad*2

	for _, section := range sections {
		caption := plot.NewTextbox("[" + section + "]")
		caption.Origin = plot.P(-1, 0)
		rowStack.Add(captionHeight, caption)
		totalHeight += captionHeight

		switch section {
		case "Iterate Objects":
			// TODO: IterateObjects doesn't have a "Scenario", so it's a special case.

		default:
			byScen := map[Scenario]*plot.AxisGroup{}

			percentileAxis := plot.NewPercentilesAxis()
			percentileAxis.Transform = plot.NewPercentileTransform(2)
			percentileAxis.Ticks = plot.ManualTicks{
				{Value: 0.25, Label: "25"},
				{Value: 0.5, Label: "50"},
				{Value: 0.75, Label: "75"},
				{Value: 0.9, Label: "90"},
				{Value: 0.95, Label: "95"},
				{Value: 0.99, Label: "99"},
			}
			yAxis := plot.NewAxis()
			yAxis.Flip = true
			yAxis.Min, yAxis.Max = 0, 0.1

			for _, parts := range partVariants {
				for _, segments := range segmentVariants {
					scenario := Scenario{Parts: parts, Segments: segments}
					axis := plot.NewAxisGroup(plot.NewGrid(), plot.NewGizmo())
					axis.X, axis.Y = percentileAxis, yAxis
					byScen[scenario] = axis
				}
			}

			for bri, br := range results {
				for _, m := range br.Measurements {
					r := m.ResultByName(section)
					if r == nil {
						continue
					}

					millis := plot.DurationTo(r.Durations, time.Millisecond)
					sort.Float64s(millis)

					percentiles := plot.NewPercentiles(br.Name, millis)
					yAxis.Max = max(yAxis.Max, percentile(millis, 0.98))

					percentiles.Stroke = palette[bri%len(palette)]
					byScen[m.Scenario].Add(percentiles)
				}
			}

			yAxis.MakeNice()

			{ // add captions to grids
				captionRow := &plot.HFlex{Margin: plot.R(pad, 0, pad, 0)}
				captionRow.Add(captionHeight, plot.Elements{})
				for _, segments := range segmentVariants {
					captionRow.Add(0, plot.NewTextbox("segments:"+strconv.Itoa(segments)))
				}
				rowStack.Add(captionHeight, captionRow)
				totalHeight += captionHeight
			}

			for _, parts := range partVariants {
				gridrow := &plot.HFlex{
					Margin: plot.R(pad, 0, pad, 0),
				}
				gridrow.Add(captionHeight, plot.NewTextbox("P:"+strconv.Itoa(parts)))

				for _, segments := range segmentVariants {
					cell := byScen[Scenario{Parts: parts, Segments: segments}]

					labels := plot.NewTickLabels()
					labels.Y.Style.Origin = plot.P(-1, 1)
					labels.X.Style.Origin = plot.P(-1, 1)

					labels.X.Style.Size = 8
					labels.Y.Style.Size = 8
					cell.Add(labels)

					gridrow.Add(0, cell)
				}

				rowStack.Add(gridCellHeight+5, gridrow)
				totalHeight += gridCellHeight + 5
			}
		}

	}

	canvas := plotsvg.New(plot.Length(columns*gridCellWidth)+captionHeight, totalHeight)
	canvas.Style += "\n.bold { font-weight: bolder; }\nsvg { background: #fff; }"
	p.Draw(canvas)

	_, err := w.Write(canvas.Bytes())
	return err
}

func max(vs ...float64) float64 {
	if len(vs) == 0 {
		return math.NaN()
	}
	m := vs[0]
	for _, v := range vs {
		if v > m {
			m = v
		}
	}
	return m
}

func percentile(sorted []float64, p float64) float64 {
	k := int(math.Ceil(p * float64(len(sorted))))
	if k >= len(sorted) {
		k = len(sorted) - 1
	}
	return sorted[k]
}

func reverseStrings(xs []string) {
	for i := len(xs)/2 - 1; i >= 0; i-- {
		k := len(xs) - 1 - i
		xs[i], xs[k] = xs[k], xs[i]
	}
}

func includeString(xs *[]string, v string) {
	for _, x := range *xs {
		if x == v {
			return
		}
	}
	*xs = append(*xs, v)
}

func includeInt(xs *[]int, v int) {
	for _, x := range *xs {
		if x == v {
			return
		}
	}
	*xs = append(*xs, v)
}

var palette = []color.Color{
	color.NRGBA{0, 200, 0, 255},
	color.NRGBA{0, 0, 200, 255},
	color.NRGBA{200, 0, 0, 255},
}
