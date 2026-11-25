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

func fastProfile(id uint16, delay time.Duration, loss float64) simnet.Profile {
	return simnet.Profile{
		PathID: id, Name: "smoke", Priority: int(id),
		RateBytesPerSec: simnet.Mbps(800), OneWayDelay: delay,
		LossRate: loss, Seed: int64(1000 + id), QueuePackets: 4096,
	}
}

func TestSmokeExactBytes(t *testing.T) {
	h := sha256.New()
	size := int64(512 * 1024)
	src := io.TeeReader(io.LimitReader(rand.New(rand.NewSource(42)), size), h)
	dst := sha256.New()

	profiles := []simnet.Profile{
		fastProfile(1, 2*time.Millisecond, 0),
		fastProfile(2, 5*time.Millisecond, 0),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	hk, rep, err := testkit.Run(ctx, testkit.Options{
		Name: "smoke", Size: size, Profiles: profiles,
		Scheduler: transport.SchedulerMinRTT, MaxSubflows: 2,
		MSS: 16 * 1024, Timeout: 15 * time.Second,
	}, src, dst)
	if err != nil {
		if hk != nil {
			t.Logf("client snapshot: %+v", hk.ClientConn.Snapshot())
		}
		t.Fatalf("transfer failed: %v", err)
	}
	defer hk.Close()
	if rep.Bytes != size {
		t.Fatalf("got %d bytes want %d", rep.Bytes, size)
	}
	if string(h.Sum(nil)) != string(dst.Sum(nil)) {
		t.Fatalf("sha256 mismatch")
	}
	t.Logf("throughput %.2f MiB/s duration %s", rep.Throughput/(1<<20), rep.Duration)
}
