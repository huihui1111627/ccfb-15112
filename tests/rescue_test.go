package tests

import (
	"bytes"
	"context"
	"testing"
	"time"

	"clocktower/multipath/internal/testkit"
	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

func TestSlowPathWindowRescue(t *testing.T) {
	src := bytes.Repeat([]byte("rescue-"), 20000) // 140 KiB
	var dst bytes.Buffer
	profiles := []simnet.Profile{
		{PathID: 1, Name: "fast-5ms", Priority: 1, RateBytesPerSec: simnet.Mbps(800), OneWayDelay: 5 * time.Millisecond, Seed: 301},
		{PathID: 2, Name: "slow-50ms", Priority: 2, RateBytesPerSec: simnet.Mbps(800), OneWayDelay: 50 * time.Millisecond, Seed: 302},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	h, rep, err := testkit.Run(ctx, testkit.Options{
		Size: int64(len(src)), Profiles: profiles, Scheduler: transport.SchedulerRoundRobin,
		MaxSubflows: 2, MSS: 8192, RcvBuffer: 64 << 10, SndBuffer: 256 << 10,
		Timeout: 20 * time.Second,
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
	}, bytes.NewReader(src), &dst)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	defer h.Close()
	if !bytes.Equal(dst.Bytes(), src) {
		t.Fatalf("byte mismatch")
	}
	s := h.ClientConn.Snapshot()
	t.Logf("duration=%s rescue=%d reinject=%d rr=%+v", rep.Duration, s.RescueEvents, s.Reinjections, s.Paths)
	if s.RescueEvents == 0 && s.Reinjections == 0 {
		t.Fatalf("expected fast-path rescue/reinjection, got rescue=%d reinject=%d", s.RescueEvents, s.Reinjections)
	}
	var slow *transport.PathSnapshot
	for i := range s.Paths {
		if s.Paths[i].PathID == 2 {
			slow = &s.Paths[i]
		}
	}
	if slow == nil {
		t.Fatal("missing slow path snapshot")
	}
}
