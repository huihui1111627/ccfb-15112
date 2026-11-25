package tests

import (
	"context"
	"crypto/sha256"
	"io"
	"math/rand"
	"testing"
	"time"

	"clocktower/multipath/internal/testkit"
	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

func TestDisconnectRedirectAndAutoRejoin(t *testing.T) {
	size := int64(4 << 20)
	h := sha256.New()
	src := io.TeeReader(io.LimitReader(rand.New(rand.NewSource(8181)), size), h)
	dst := sha256.New()
	profiles := []simnet.Profile{
		{PathID: 1, Name: "stable", Priority: 1, RateBytesPerSec: simnet.Mbps(250), OneWayDelay: 5 * time.Millisecond, Seed: 81},
		{PathID: 2, Name: "flaky", Priority: 2, RateBytesPerSec: simnet.Mbps(180), OneWayDelay: 20 * time.Millisecond, Seed: 82, FlapEvery: 120 * time.Millisecond, FlapDown: 120 * time.Millisecond},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	hk, _, err := testkit.Run(ctx, testkit.Options{
		Size: size, Profiles: profiles, Scheduler: transport.SchedulerRoundRobin, MaxSubflows: 2, Timeout: 30 * time.Second,
		OnReady: func(h *testkit.Harness) {
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				for _, p := range h.ClientConn.Snapshot().Paths {
					if p.PathID == 2 && p.Status == "active" {
						return
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		},
	}, src, dst)
	if err != nil {
		t.Fatal(err)
	}
	defer hk.Close()
	if string(dst.Sum(nil)) != string(h.Sum(nil)) {
		t.Fatal("byte stream mismatch")
	}
	deadline := time.Now().Add(2 * time.Second)
	var snap *transport.Snapshot
	for time.Now().Before(deadline) {
		snap = hk.ClientConn.Snapshot()
		for _, p := range snap.Paths {
			if p.PathID == 2 && p.Rejoins > 0 {
				break
			}
		}
		if flakyHasRejoin(snap, 2) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var flaky *transport.PathSnapshot
	for i := range snap.Paths {
		if snap.Paths[i].PathID == 2 {
			flaky = &snap.Paths[i]
		}
	}
	if flaky == nil || flaky.DownEvents == 0 || flaky.Rejoins == 0 {
		t.Fatalf("expected disconnect and rejoin, got %+v", flaky)
	}
}

func flakyHasRejoin(s *transport.Snapshot, id uint16) bool {
	if s == nil {
		return false
	}
	for _, p := range s.Paths {
		if p.PathID == id && p.Rejoins > 0 {
			return true
		}
	}
	return false
}
