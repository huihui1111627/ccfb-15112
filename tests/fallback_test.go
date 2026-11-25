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

func TestMiddleboxFallback(t *testing.T) {
	for _, mode := range []simnet.MiddleboxMode{simnet.MiddleboxMutateOption, simnet.MiddleboxSplitMapping} {
		t.Run(string(mode), func(t *testing.T) {
			src := bytes.NewReader(bytes.Repeat([]byte("clocktower-mapping-"), 4096))
			var dst bytes.Buffer
			profiles := []simnet.Profile{
				{PathID: 1, Name: "clean-primary", Priority: 1, RateBytesPerSec: simnet.Mbps(800), OneWayDelay: 2 * time.Millisecond, Seed: 201},
				{PathID: 2, Name: "middlebox", Priority: 2, RateBytesPerSec: simnet.Mbps(800), OneWayDelay: 2 * time.Millisecond, Seed: 202, Middlebox: mode},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			h, _, err := testkit.Run(ctx, testkit.Options{
				Size: int64(src.Len()), Profiles: profiles, Scheduler: transport.SchedulerRoundRobin,
				MaxSubflows: 2, Timeout: 15 * time.Second, MSS: 4096,
			}, src, &dst)
			if err != nil {
				t.Fatalf("transfer: %v", err)
			}
			defer h.Close()
			if !bytes.Equal(dst.Bytes(), bytes.Repeat([]byte("clocktower-mapping-"), 4096)) {
				t.Fatalf("byte stream mismatch")
			}
			snap := h.ClientConn.Snapshot()
			if snap.Mode != "single-path-fallback" {
				for _, p := range snap.Paths {
					t.Logf("path %+v", p)
				}
				t.Logf("mappings=%+v", snap.Mappings)
				t.Logf("events=%+v", snap.Events)
				t.Fatalf("expected fallback mode, got %s reason=%q", snap.Mode, snap.FallbackReason)
			}
		})
	}
}
