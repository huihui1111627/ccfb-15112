package tests

import (
	"context"
	"crypto/sha256"
	"hash"
	"io"
	"math/rand"
	"testing"
	"time"

	"clocktower/multipath/internal/scenarios"
	"clocktower/multipath/internal/testkit"
	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

func seededSource(seed int64, size int64) (io.Reader, hash.Hash) {
	h := sha256.New()
	r := io.TeeReader(io.LimitReader(rand.New(rand.NewSource(seed)), size), h)
	return r, h
}

func TestDozensOfLinkCombinationsExactAndGoodput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping matrix in -short mode")
	}
	const sizeMB = 2
	const size = int64(sizeMB) << 20
	cases := scenarios.All()
	if len(cases) < 30 {
		t.Fatalf("expected dozens of scenarios, got %d", len(cases))
	}

	for i, sc := range cases {
		t.Run(sc.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			var bestSingle float64
			for j, bp := range sc.Profiles {
				baseSrc, _ := seededSource(int64(i)*100+int64(j), size)
				baseHash := sha256.New()
				bh, rep, err := testkit.Run(ctx, testkit.Options{
					Size: size, Profiles: []simnet.Profile{bp}, Scheduler: transport.SchedulerMinRTT,
					MaxSubflows: 1, Timeout: 30 * time.Second,
				}, baseSrc, baseHash)
				if bh != nil {
					bh.Close()
				}
				if err != nil {
					// A deliberately extreme candidate can exceed the small
					// matrix timeout; bestSingle below catches total failure.
					continue
				}
				if rep.Throughput > bestSingle {
					bestSingle = rep.Throughput
				}
			}

			if bestSingle == 0 {
				t.Fatalf("every standalone baseline path failed")
			}

			src, sourceHash := seededSource(int64(i)*100+1, size)
			gotHash := sha256.New()
			h, mp, err := testkit.Run(ctx, testkit.Options{
				Size: size, Profiles: sc.Profiles, Scheduler: transport.SchedulerMinRTT,
				MaxSubflows: len(sc.Profiles), Timeout: 30 * time.Second,
			}, src, gotHash)
			if err != nil {
				t.Fatalf("multipath: %v", err)
			}
			defer h.Close()
			if mp.Bytes != size {
				t.Fatalf("received %d bytes, want %d", mp.Bytes, size)
			}
			if string(gotHash.Sum(nil)) != string(sourceHash.Sum(nil)) {
				t.Fatalf("byte stream mismatch")
			}
			ratio := mp.Throughput / bestSingle
			t.Logf("best-single=%.2f MB/s mp=%.2f MB/s ratio=%.2f", bestSingle/1e6, mp.Throughput/1e6, ratio)
			// The automated Go matrix uses small 2 MiB cases to stay fast.
			// Startup/RTO tails dominate there; the standalone mptest program
			// runs larger transfers and enforces per-scenario 0.5-0.8 ratios.
			if ratio < 0.40 {
				snap := h.ClientConn.Snapshot()
				for _, p := range snap.Paths {
					t.Logf("path %+v", p)
				}
				t.Logf("client events %+v", snap.Events)
				t.Fatalf("goodput ratio %.2f below 0.40 smoke floor", ratio)
			}
		})
	}
}
