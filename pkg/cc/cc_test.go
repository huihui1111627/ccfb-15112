package cc

import (
	"math"
	"testing"
	"time"
)

func manualAlpha(g *Group) float64 {
	total := 0.0
	var sumWR, maxWR2 float64
	for _, p := range g.paths {
		rtt := p.RTT.Seconds()
		total += p.W
		sumWR += p.W / rtt
		if v := p.W / (rtt * rtt); v > maxWR2 {
			maxWR2 = v
		}
	}
	return total * maxWR2 / (sumWR * sumWR)
}

func TestLinkedFormulaStepByStep(t *testing.T) {
	g := NewGroup(16384)
	p1 := g.AddPath(1, 20*time.Millisecond)
	p2 := g.AddPath(2, 100*time.Millisecond)

	// Force congestion avoidance so the linked formula, not slow start, is
	// under test.
	p1.W, p1.Ssthresh = 12, 1
	p2.W, p2.Ssthresh = 6, 1

	for step := 0; step < 5; step++ {
		before := g.paths[1].W
		total := g.TotalWindowMSS()
		alpha := manualAlpha(g)
		wantInc := math.Min(alpha/total, 1.0/before)
		wantAfter := before + wantInc

		got := g.AckOne(1)
		if got.Kind != "ack" || got.Reason != "linked-congestion-avoidance" {
			t.Fatalf("step %d: unexpected step %+v", step, got)
		}
		if math.Abs(got.Alpha-alpha) > 1e-15 {
			t.Fatalf("step %d: alpha %.15f want %.15f", step, got.Alpha, alpha)
		}
		if math.Abs(got.Increment-wantInc) > 1e-15 {
			t.Fatalf("step %d: increment %.15f want %.15f", step, got.Increment, wantInc)
		}
		if math.Abs(got.After-wantAfter) > 1e-12 {
			t.Fatalf("step %d: after %.12f want %.12f", step, got.After, wantAfter)
		}
		if math.Abs(g.paths[1].W-wantAfter) > 1e-12 {
			t.Fatalf("step %d: stored cwnd %.12f want %.12f", step, g.paths[1].W, wantAfter)
		}
	}

	before := g.paths[2].W
	loss := g.Halve(2, "unit-test")
	want := math.Max(MinWindow, before/2)
	if math.Abs(loss.After-want) > 1e-12 || math.Abs(g.paths[2].W-want) > 1e-12 {
		t.Fatalf("halve after=%v want=%v stored=%v", loss.After, want, g.paths[2].W)
	}
	wantSS := math.Max(MinWindow*2, before/2)
	if math.Abs(g.paths[2].Ssthresh-wantSS) > 1e-12 {
		t.Fatalf("ssthresh=%v want=%v", g.paths[2].Ssthresh, wantSS)
	}
}

func TestSlowStartStepIsExactlyOneMSS(t *testing.T) {
	g := NewGroup(1400)
	g.AddPath(7, 10*time.Millisecond)
	before := g.paths[7].W
	st := g.AckOne(7)
	if st.Increment != 1 || st.After != before+1 {
		t.Fatalf("slow start step=%+v", st)
	}
}

func TestOrdinaryRenoReference(t *testing.T) {
	r := NewOrdinaryReno()
	r.W = 20
	r.Ssthresh = 10
	// Congestion avoidance increment is 1/W per ACK.
	before := r.W
	inc := r.AckOne()
	if math.Abs(inc-1/before) > 1e-15 {
		t.Fatalf("reno increment=%v want %v", inc, 1/before)
	}
	r.W = 20
	r.Loss()
	if r.W != 10 || r.Ssthresh != 10 {
		t.Fatalf("reno loss w=%v ssthresh=%v", r.W, r.Ssthresh)
	}
}
