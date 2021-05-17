// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/loov/hrtime"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/common/testrand"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
)

// Scenario defines arguments for an object.
type Scenario struct {
	Parts    int
	Segments int
}

// Benchmark contains the configuration and state of the benchmark.
type Benchmark struct {
	DBURL       string
	Count       int
	MaxDuration time.Duration

	ProjectID  uuid.UUID
	BucketName string

	Redundancy      storj.RedundancyScheme
	SegmentVariants []int
	PartsVariants   []int

	Objects map[Scenario][]metabase.ObjectLocation
}

// NewBenchmark creates a benchmark with default values.
func NewBenchmark(dburl string) *Benchmark {
	return &Benchmark{
		DBURL:       dburl,
		Count:       50,
		MaxDuration: 2 * time.Minute,

		ProjectID:  testrand.UUID(),
		BucketName: "benchmark",

		Redundancy: storj.RedundancyScheme{
			Algorithm:      storj.ReedSolomon,
			RequiredShares: 29,
			RepairShares:   50,
			OptimalShares:  85,
			TotalShares:    90,
			ShareSize:      256,
		},
		SegmentVariants: []int{0, 1, 2, 3, 11},
		PartsVariants:   []int{1, 2, 10},

		Objects: map[Scenario][]metabase.ObjectLocation{},
	}
}

// Scenarios returns all scenarios that should be examined.
func (b *Benchmark) Scenarios() []Scenario {
	var xs []Scenario
	for _, parts := range b.PartsVariants {
		for _, segments := range b.SegmentVariants {
			xs = append(xs, Scenario{Parts: parts, Segments: segments})
		}
	}
	return xs
}

// Run runs all benchmarks.
func (b *Benchmark) Run(ctx context.Context, log *zap.Logger) ([]Measurement, error) {
	db, err := metabase.Open(ctx, log, b.DBURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open metabase: %w", err)
	}
	defer func() { _ = db.Close() }()

	err = db.MigrateToLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to migrate metabase: %w", err)
	}

	measurements := []Measurement{}

	for _, scenario := range b.Scenarios() {
		measurement, err := b.Upload(ctx, db, scenario)
		if err != nil {
			return nil, fmt.Errorf("upload failed: %w", err)
		}
		measurements = append(measurements, measurement)
	}

	measurement, err := b.Iterate(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("iterate failed: %w", err)
	}
	measurements = append(measurements, measurement)

	for _, scenario := range b.Scenarios() {
		measurement, err := b.ListSegments(ctx, db, scenario)
		if err != nil {
			return nil, fmt.Errorf("list segments failed: %w", err)
		}
		measurements = append(measurements, measurement)
	}

	for _, scenario := range b.Scenarios() {
		measurement, err := b.Download(ctx, db, scenario)
		if err != nil {
			return nil, fmt.Errorf("download failed: %w", err)
		}
		measurements = append(measurements, measurement)
	}

	for _, scenario := range b.Scenarios() {
		measurement, err := b.Delete(ctx, db, scenario)
		if err != nil {
			return nil, fmt.Errorf("delete failed: %w", err)
		}
		measurements = append(measurements, measurement)
	}

	return measurements, nil
}

// Upload runs upload object benchmarks with given number of parts and segments.
func (b *Benchmark) Upload(ctx context.Context, db *metabase.DB, scenario Scenario) (Measurement, error) {
	fmt.Printf("Benchmark Upload (Parts:%d, Segments:%d): ", scenario.Parts, scenario.Segments)
	defer fmt.Println()

	measurement := Measurement{Scenario: scenario}

	objects := b.Objects[scenario]
	defer func() { b.Objects[scenario] = objects }()

	// all but the last segment should be remote segments
	remoteSegments := 0
	if scenario.Segments > 1 {
		remoteSegments = scenario.Segments - 1
	}

	// the last segment should be inline
	inlineSegments := 0
	if scenario.Segments > 0 {
		inlineSegments = 1
	}

	start := time.Now()
	for k := 0; k < b.Count; k++ {
		if time.Since(start) > b.MaxDuration {
			break
		}
		fmt.Print(".")

		objectStream := metabase.ObjectStream{
			ProjectID:  b.ProjectID,
			BucketName: b.BucketName,
			ObjectKey:  metabase.ObjectKey(testrand.Path() + "/" + testrand.UUID().String()),
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
				return measurement, fmt.Errorf("begin object failed: %w", err)
			}
			finish := hrtime.Now()
			measurement.Record("Begin Object", finish-start)
		}

		{ // uploads parts in parallel
			g, ctx := errgroup.WithContext(ctx)
			for p := 0; p < scenario.Parts; p++ {
				p := p
				g.Go(func() error {
					for r := 0; r < remoteSegments; r++ {
						rootPieceID := testrand.PieceID()
						pieces := randPieces(int(b.Redundancy.OptimalShares))

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
								return fmt.Errorf("begin remote segment failed: %w", err)
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
								Redundancy:        b.Redundancy,
							})
							if err != nil {
								return fmt.Errorf("commit remote segment failed: %w", err)
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
							return fmt.Errorf("commit inline segment failed: %w", err)
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
				return measurement, fmt.Errorf("commit object failed: %w", err)
			}
			finish := hrtime.Now()
			measurement.Record("Commit Object", finish-start)
		}

		totalFinish := hrtime.Now()
		measurement.Record("Upload Total", totalFinish-totalStart)
	}

	return measurement, nil
}

// Iterate runs list bucket benchmarks on the full benchmark bucket.
func (b *Benchmark) Iterate(ctx context.Context, db *metabase.DB) (Measurement, error) {
	fmt.Printf("Benchmark Iterate: ")
	defer fmt.Println()

	measurement := Measurement{}

	start := time.Now()
	for k := 0; k < b.Count; k++ {
		if time.Since(start) > b.MaxDuration {
			break
		}
		fmt.Print(".")
		start := hrtime.Now()

		err := db.IterateObjectsAllVersions(ctx, metabase.IterateObjects{
			ProjectID:  b.ProjectID,
			BucketName: b.BucketName,
		}, func(ctx context.Context, it metabase.ObjectsIterator) error {
			var entry metabase.ObjectEntry
			for it.Next(ctx, &entry) {
			}
			return nil
		})
		if err != nil {
			return measurement, fmt.Errorf("iterate objects failed: %w", err)
		}
		finish := hrtime.Now()
		measurement.Record("Iterate Objects", finish-start)
	}

	return measurement, nil
}

// ListSegments runs list segments benchmarks of objects with given number of parts and segments.
func (b *Benchmark) ListSegments(ctx context.Context, db *metabase.DB, scenario Scenario) (Measurement, error) {
	fmt.Printf("Benchmark ListSegments (Parts:%d, Segments:%d): ", scenario.Parts, scenario.Segments)
	defer fmt.Println()

	measurement := Measurement{Scenario: scenario}
	objects := b.Objects[scenario]

	for _, location := range objects {
		fmt.Print(".")

		// get object
		object, err := db.GetObjectLatestVersion(ctx, metabase.GetObjectLatestVersion{
			ObjectLocation: location,
		})
		if err != nil {
			return measurement, fmt.Errorf("get object failed: %w", err)
		}

		// list object's segments
		start := hrtime.Now()
		for {
			result, err := db.ListSegments(ctx, metabase.ListSegments{
				StreamID: object.StreamID,
			})
			if err != nil {
				return measurement, fmt.Errorf("list segment failed: %w", err)
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

// Download runs download object benchmarks with given number of parts and segments.
func (b *Benchmark) Download(ctx context.Context, db *metabase.DB, scenario Scenario) (Measurement, error) {
	fmt.Printf("Benchmark Download (Parts:%d, Segments:%d): ", scenario.Parts, scenario.Segments)
	defer fmt.Println()

	measurement := Measurement{Scenario: scenario}
	objects := b.Objects[scenario]

	for _, location := range objects {
		fmt.Print(".")
		totalStart := hrtime.Now()

		// get object
		start := hrtime.Now()
		object, err := db.GetObjectLatestVersion(ctx, metabase.GetObjectLatestVersion{
			ObjectLocation: location,
		})
		if err != nil {
			return measurement, fmt.Errorf("get object failed: %w", err)
		}
		finish := hrtime.Now()
		measurement.Record("Get Object", finish-start)

		for p := 0; p < scenario.Parts; p++ {
			for i := 0; i < scenario.Segments; i++ {
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
					return measurement, fmt.Errorf("get segment failed: %w", err)
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

// Delete runs delete object benchmarks with given number of parts and segments.
func (b *Benchmark) Delete(ctx context.Context, db *metabase.DB, scenario Scenario) (Measurement, error) {
	fmt.Printf("Benchmark Delete (Parts:%d, Segments:%d): ", scenario.Parts, scenario.Segments)
	defer fmt.Println()

	measurement := Measurement{Scenario: scenario}
	objects := b.Objects[scenario]

	for _, location := range objects {
		fmt.Print(".")
		// delete object
		start := hrtime.Now()
		_, err := db.DeleteObjectLatestVersion(ctx, metabase.DeleteObjectLatestVersion{
			ObjectLocation: location,
		})
		if err != nil {
			return measurement, fmt.Errorf("delete object failed: %w", err)
		}
		finish := hrtime.Now()
		measurement.Record("Delete Object", finish-start)
	}

	return measurement, nil
}

func randPieces(count int) metabase.Pieces {
	pieces := make(metabase.Pieces, count)
	for i := range pieces {
		pieces[i] = metabase.Piece{
			Number:      uint16(i),
			StorageNode: testrand.NodeID(),
		}
	}
	return pieces
}
