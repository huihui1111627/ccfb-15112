package tests

import (
	"context"
	"testing"
	"time"

	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

func TestMaxSubflowsAndPriorityRefill(t *testing.T) {
	addrs := make([]transport.Address, 0, 4)
	serverAddrs := make([]transport.Address, 0, 4)
	for i := uint16(1); i <= 4; i++ {
		pr := simnet.Profile{
			PathID: i, Name: "priority", Priority: int(i),
			RateBytesPerSec: simnet.Mbps(500), OneWayDelay: time.Millisecond, Seed: int64(i),
		}
		pair := simnet.NewPair(pr)
		addrs = append(addrs, transport.Address{PathID: i, Priority: int(i), Link: pair.Client})
		serverAddrs = append(serverAddrs, transport.Address{PathID: i, Priority: int(i), Link: pair.Server})
	}
	cfg := transport.DefaultConfig()
	cfg.MaxSubflows = 2
	cfg.HandshakeTimeout = 10 * time.Second
	srv := transport.Listen(cfg, serverAddrs)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := transport.Dial(ctx, cfg, addrs)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sc, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()

	wait := func(wantPath uint16, wantQueued int) {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			s := c.Snapshot()
			has := false
			for _, p := range s.Paths {
				if p.PathID == wantPath && p.Status == "active" {
					has = true
				}
			}
			if has && s.QueuedAddrs == wantQueued {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		s := c.Snapshot()
		t.Fatalf("timeout waiting path %d queued %d: paths=%+v queued=%+v", wantPath, wantQueued, s.Paths, s.QueuedAddresses)
	}
	wait(2, 2) // primary 1 + first auxiliary 2; addresses 3 and 4 are recorded but not dialed.

	if err := c.CloseSubflow(2); err != nil {
		t.Fatal(err)
	}
	wait(3, 1) // priority refill creates path 3; path 4 remains queued.
}
