// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/loov/hrtime"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testrand"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metainfo"
	"storj.io/storj/satellite/metainfo/metabase"
)

type testScenario struct {
	parts    int
	segments int
}

var (
	testObjects = make(map[testScenario][]metabase.ObjectLocation)
	redundancy  storj.RedundancyScheme
)

func main() {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("Failed to created logger: %v\n", err)
		os.Exit(1)
	}

	redundancy = storj.RedundancyScheme{
		Algorithm:      storj.ReedSolomon,
		RequiredShares: 29,
		RepairShares:   50,
		OptimalShares:  85,
		TotalShares:    90,
		ShareSize:      256,
	}

	dburl := flag.String("database-url", "postgres://postgres@localhost/benchmark?sslmode=disable", "database url")
	count := flag.Int("count", 50, "benchmark count")
	duration := flag.Duration("time", 2*time.Minute, "maximum benchmark time per filesize")
	flag.Parse()

	db, err := metainfo.OpenMetabase(ctx, log, *dburl)
	if err != nil {
		log.Fatal("Failed to open metabase.", zap.Error(err))
	}

	err = db.MigrateToLatest(ctx)
	if err != nil {
		log.Fatal("Failed to migrate metabase.", zap.Error(err))
	}

	projectID := testrand.UUID()
	bucketName := "benchmark"

	measurements := []Measurement{}
	segmentsVariants := []int{0, 1, 2, 3, 11}
	partsVariants := []int{1, 2, 10}

	for _, parts := range partsVariants {
		for _, segments := range segmentsVariants {
			measurement, err := UploadObjectBenchmark(ctx, db, projectID, bucketName, parts, segments, *count, *duration)
			if err != nil {
				log.Fatal("Benchmark failed.", zap.Error(err))
			}
			measurements = append(measurements, measurement)
		}
	}

	measurement, err := IterateObjectsBenchmark(ctx, db, projectID, bucketName, *count, *duration)
	if err != nil {
		log.Fatal("Benchmark failed.", zap.Error(err))
	}
	measurements = append(measurements, measurement)

	for _, parts := range partsVariants {
		for _, segments := range segmentsVariants {
			measurement, err := ListSegmentsBenchmark(ctx, db, projectID, bucketName, parts, segments)
			if err != nil {
				log.Fatal("Benchmark failed.", zap.Error(err))
			}
			measurements = append(measurements, measurement)
		}
	}

	for _, parts := range partsVariants {
		for _, segments := range segmentsVariants {
			measurement, err := DownloadObjectBenchmark(ctx, db, projectID, bucketName, parts, segments)
			if err != nil {
				log.Fatal("Benchmark failed.", zap.Error(err))
			}
			measurements = append(measurements, measurement)
		}
	}

	for _, parts := range partsVariants {
		for _, segments := range segmentsVariants {
			measurement, err := DeleteObjectBenchmark(ctx, db, projectID, bucketName, parts, segments)
			if err != nil {
				log.Fatal("Benchmark failed.", zap.Error(err))
			}
			measurements = append(measurements, measurement)
		}
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

	// if *plotname != "" {
	// 	err := Plot(*plotname, measurements)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
}

// Measurement contains measurements for different requests.
type Measurement struct {
	Parts    int
	Segments int
	Results  []*Result
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

// UploadObjectBenchmark runs upload object benchmarks with given number of parts and segments.
func UploadObjectBenchmark(ctx context.Context, db metainfo.MetabaseDB, projectID uuid.UUID, bucketName string, parts, segments, count int, duration time.Duration) (Measurement, error) {
	fmt.Printf("Benchmarking upload object with %d parts, %d segments each ", parts, segments)
	defer fmt.Println()

	measurement := Measurement{}
	measurement.Parts = parts
	measurement.Segments = segments

	objects := testObjects[testScenario{parts: parts, segments: segments}]
	defer func() {
		testObjects[testScenario{parts: parts, segments: segments}] = objects
	}()

	// all but the last segment should be remote segments
	remoteSegments := 0
	if segments > 1 {
		remoteSegments = segments - 1
	}

	// the last segment should be inline
	inlineSegments := 0
	if segments > 0 {
		inlineSegments = 1
	}

	start := time.Now()
	for k := 0; k < count; k++ {
		if time.Since(start) > duration {
			break
		}
		fmt.Print(".")

		objectStream := metabase.ObjectStream{
			ProjectID:  projectID,
			BucketName: bucketName,
			ObjectKey:  metabase.ObjectKey(testrand.Path()),
			Version:    1,
			StreamID:   testrand.UUID(),
		}
		objects = append(objects, objectStream.Location())

		totalStart := hrtime.Now()

		{ // begin object
			start := hrtime.Now()
			_, err := db.BeginObjectExactVersion(ctx, metabase.BeginObjectExactVersion{
				ObjectStream: objectStream,
				Encryption: storj.EncryptionParameters{
					CipherSuite: storj.EncAESGCM,
					BlockSize:   256,
				},
			})
			if err != nil {
				return measurement, fmt.Errorf("begin object failed: %+v", err)
			}
			finish := hrtime.Now()
			measurement.Record("Begin Object", finish-start)
		}

		{ // uploads parts in parallel
			g, ctx := errgroup.WithContext(ctx)
			for p := 0; p < parts; p++ {
				p := p
				g.Go(func() error {
					for r := 0; r < remoteSegments; r++ {
						rootPieceID := testrand.PieceID()
						pieces := randPieces()

						{ // begin remote segment
							start := hrtime.Now()
							err := db.BeginSegment(ctx, metabase.BeginSegment{
								ObjectStream: objectStream,
								Position: metabase.SegmentPosition{
									Part:  uint32(p),
									Index: uint32(r),
								},
								RootPieceID: rootPieceID,
								Pieces:      pieces,
							})
							if err != nil {
								return fmt.Errorf("begin remote segment failed: %+v", err)
							}
							finish := hrtime.Now()
							measurement.Record("Begin Remote Segment", finish-start)
						}

						{ // commit remote segment
							start := hrtime.Now()
							segmentSize := testrand.Intn(64*memory.MiB.Int()) + 1
							err := db.CommitSegment(ctx, metabase.CommitSegment{
								ObjectStream: objectStream,
								Position: metabase.SegmentPosition{
									Part:  uint32(p),
									Index: uint32(r),
								},
								EncryptedKey:      testrand.BytesInt(storj.KeySize),
								EncryptedKeyNonce: testrand.BytesInt(storj.NonceSize),
								PlainSize:         int32(segmentSize),
								EncryptedSize:     int32(segmentSize),
								RootPieceID:       rootPieceID,
								Pieces:            pieces,
								Redundancy:        redundancy,
							})
							if err != nil {
								return fmt.Errorf("commit remote segment failed: %+v", err)
							}
							finish := hrtime.Now()
							measurement.Record("Commit Remote Segment", finish-start)
						}
					}

					for i := 0; i < inlineSegments; i++ {
						// commit inline segment
						start := hrtime.Now()
						segmentSize := testrand.Intn(4*memory.KiB.Int()) + 1
						err := db.CommitInlineSegment(ctx, metabase.CommitInlineSegment{
							ObjectStream: objectStream,
							Position: metabase.SegmentPosition{
								Part:  uint32(p),
								Index: uint32(remoteSegments + i),
							},
							InlineData:        testrand.BytesInt(segmentSize),
							EncryptedKey:      testrand.BytesInt(storj.KeySize),
							EncryptedKeyNonce: testrand.BytesInt(storj.NonceSize),
							PlainSize:         int32(segmentSize),
						})
						if err != nil {
							return fmt.Errorf("commit inline segment failed: %+v", err)
						}
						finish := hrtime.Now()
						measurement.Record("Commit Inline Segment", finish-start)
					}

					return nil
				})
				if err := g.Wait(); err != nil {
					return measurement, err
				}
			}
		}

		{ // commit object
			start := hrtime.Now()
			_, err := db.CommitObject(ctx, metabase.CommitObject{
				ObjectStream: objectStream,
			})
			if err != nil {
				return measurement, fmt.Errorf("commit object failed: %+v", err)
			}
			finish := hrtime.Now()
			measurement.Record("Commit Object", finish-start)
		}

		totalFinish := hrtime.Now()
		measurement.Record("Upload Total", totalFinish-totalStart)
	}

	return measurement, nil
}

// IterateObjectsBenchmark runs list bucket benchmarks on the full benchmark bucket.
func IterateObjectsBenchmark(ctx context.Context, db metainfo.MetabaseDB, projectID uuid.UUID, bucketName string, count int, duration time.Duration) (Measurement, error) {
	fmt.Print("Benchmarking iterate objects ")
	defer fmt.Println()

	measurement := Measurement{}

	start := time.Now()
	for k := 0; k < count; k++ {
		if time.Since(start) > duration {
			break
		}
		fmt.Print(".")
		start := hrtime.Now()

		err := db.IterateObjectsAllVersions(ctx, metabase.IterateObjects{
			ProjectID:  projectID,
			BucketName: bucketName,
		}, func(ctx context.Context, it metabase.ObjectsIterator) error {
			var entry metabase.ObjectEntry
			for it.Next(ctx, &entry) {
			}
			return nil
		})
		if err != nil {
			return measurement, fmt.Errorf("iterate objects failed: %+v", err)
		}
		finish := hrtime.Now()
		measurement.Record("Iterate Objects", finish-start)
	}

	return measurement, nil
}

// ListSegmentsBenchmark runs list segments benchmarks of objects with given number of parts and segments.
func ListSegmentsBenchmark(ctx context.Context, db metainfo.MetabaseDB, projectID uuid.UUID, bucketName string, parts, segments int) (Measurement, error) {
	fmt.Printf("Benchmarking list segments of objects with %d parts, %d segments each ", parts, segments)
	defer fmt.Println()

	measurement := Measurement{}
	measurement.Parts = parts
	measurement.Segments = segments

	objects := testObjects[testScenario{parts: parts, segments: segments}]

	for _, location := range objects {
		fmt.Print(".")

		// get object
		object, err := db.GetObjectLatestVersion(ctx, metabase.GetObjectLatestVersion{
			ObjectLocation: location,
		})
		if err != nil {
			return measurement, fmt.Errorf("get object failed: %+v", err)
		}

		// list object's segments
		start := hrtime.Now()
		for {
			result, err := db.ListSegments(ctx, metabase.ListSegments{
				StreamID: object.StreamID,
			})
			if err != nil {
				return measurement, fmt.Errorf("list segment failed: %+v", err)
			}
			if !result.More {
				break
			}
		}
		finish := hrtime.Now()
		measurement.Record("List Segments", finish-start)
	}

	return measurement, nil
}

// DownloadObjectBenchmark runs download object benchmarks with given number of parts and segments.
func DownloadObjectBenchmark(ctx context.Context, db metainfo.MetabaseDB, projectID uuid.UUID, bucketName string, parts, segments int) (Measurement, error) {
	fmt.Printf("Benchmarking download object with %d parts, %d segments each ", parts, segments)
	defer fmt.Println()

	measurement := Measurement{}
	measurement.Parts = parts
	measurement.Segments = segments

	objects := testObjects[testScenario{parts: parts, segments: segments}]

	for _, location := range objects {
		fmt.Print(".")
		totalStart := hrtime.Now()

		// get object
		start := hrtime.Now()
		object, err := db.GetObjectLatestVersion(ctx, metabase.GetObjectLatestVersion{
			ObjectLocation: location,
		})
		if err != nil {
			return measurement, fmt.Errorf("get object failed: %+v", err)
		}
		finish := hrtime.Now()
		measurement.Record("Get Object", finish-start)

		for p := 0; p < parts; p++ {
			for i := 0; i < segments; i++ {
				// get segment
				start := hrtime.Now()
				_, err = db.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
					StreamID: object.StreamID,
					Position: metabase.SegmentPosition{
						Part:  uint32(p),
						Index: uint32(i),
					},
				})
				if err != nil {
					return measurement, fmt.Errorf("get segment failed: %+v", err)
				}
				finish := hrtime.Now()
				measurement.Record("Get Segment", finish-start)
			}
		}

		totalFinish := hrtime.Now()
		measurement.Record("Download Total", totalFinish-totalStart)
	}

	return measurement, nil
}

// DeleteObjectBenchmark runs delete object benchmarks with given number of parts and segments.
func DeleteObjectBenchmark(ctx context.Context, db metainfo.MetabaseDB, projectID uuid.UUID, bucketName string, parts, segments int) (Measurement, error) {
	fmt.Printf("Benchmarking delete object with %d parts, %d segments each ", parts, segments)
	defer fmt.Println()

	measurement := Measurement{}
	measurement.Parts = parts
	measurement.Segments = segments

	objects := testObjects[testScenario{parts: parts, segments: segments}]

	for _, location := range objects {
		fmt.Print(".")
		// delete object
		start := hrtime.Now()
		_, err := db.DeleteObjectLatestVersion(ctx, metabase.DeleteObjectLatestVersion{
			ObjectLocation: location,
		})
		if err != nil {
			return measurement, fmt.Errorf("delete object failed: %+v", err)
		}
		finish := hrtime.Now()
		measurement.Record("Delete Object", finish-start)
	}

	return measurement, nil
}

func randPieces() metabase.Pieces {
	pieces := make(metabase.Pieces, int(redundancy.OptimalShares))

	for i := range pieces {
		pieces[i] = metabase.Piece{
			Number:      uint16(i),
			StorageNode: testrand.NodeID(),
		}
	}

	return pieces
}
