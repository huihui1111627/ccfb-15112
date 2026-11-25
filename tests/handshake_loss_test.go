package tests

import (
	"context"
	"testing"
	"time"

	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

func TestHighLossHandshake(t *testing.T) {
	for attempt := 0; attempt < 5; attempt++ {
		pr := simnet.Profile{PathID: 9, Name: "loss", Priority: 1, RateBytesPerSec: simnet.Mbps(500), OneWayDelay: 7 * time.Millisecond, LossRate: 0.22, Seed: int64(999 + attempt)}
		pair := simnet.NewPair(pr)
		cfg := transport.DefaultConfig()
		cfg.HandshakeTimeout = 15 * time.Second
		srv := transport.Listen(cfg, []transport.Address{{PathID: 9, Link: pair.Server}})
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		c, err := transport.Dial(ctx, cfg, []transport.Address{{PathID: 9, Link: pair.Client}})
		if err != nil {
			cancel()
			t.Fatalf("attempt %d dial: %v", attempt, err)
		}
		sc, err := srv.Accept(ctx)
		if err != nil {
			snap := c.Snapshot()
			t.Fatalf("attempt %d accept: %v client paths=%+v events=%+v", attempt, err, snap.Paths, snap.Events)
		}
		t.Logf("attempt %d ok", attempt)
		_ = c.Close()
		_ = sc.Close()
		srv.Close()
		cancel()
	}
}
