// Package scenarios generates deterministic link combinations for matrix tests
// and the dashboard comparison runner.
package scenarios

import (
	"fmt"
	"time"

	"clocktower/multipath/pkg/simnet"
)

type Scenario struct {
	Name        string
	Description string
	Profiles    []simnet.Profile
	// Ratio is the minimum multipath/best-single-path goodput ratio asserted.
	Ratio float64
}

type linkOpts struct {
	id       uint16
	delayMS  int
	mbps     float64
	loss     float64
	priority int
	flap     bool
	middle   simnet.MiddleboxMode
}

func profile(o linkOpts) simnet.Profile {
	p := simnet.Profile{
		PathID:          o.id,
		Name:            fmt.Sprintf("p%d-%dms-%.0fmbps-loss%.3f", o.id, o.delayMS, o.mbps, o.loss),
		Priority:        o.priority,
		RateBytesPerSec: simnet.Mbps(o.mbps),
		OneWayDelay:     time.Duration(o.delayMS) * time.Millisecond,
		LossRate:        o.loss,
		Seed:            9100 + int64(o.id)*137 + int64(o.delayMS)*17 + int64(o.loss*1000),
		Middlebox:       o.middle,
	}
	if o.flap {
		p.FlapEvery = 220 * time.Millisecond
		p.FlapDown = 90 * time.Millisecond
	}
	return p
}

// All returns several dozen deterministic combinations. The matrix deliberately
// includes ten-fold RTT asymmetry, frequent flaps and high loss.
func All() []Scenario {
	var out []Scenario
	add := func(name, desc string, ratio float64, ops ...linkOpts) {
		ps := make([]simnet.Profile, 0, len(ops))
		for _, o := range ops {
			ps = append(ps, profile(o))
		}
		out = append(out, Scenario{Name: name, Description: desc, Profiles: ps, Ratio: ratio})
	}

	// Latency ratio matrix.
	for i, slowMS := range []int{6, 20, 60} { // 1x-ish, ~3x, 10x versus 6ms
		add(fmt.Sprintf("latency-ratio-%d", i+1),
			fmt.Sprintf("6ms fast path versus %dms slow path", slowMS), 0.75,
			linkOpts{id: 1, delayMS: 6, mbps: 600, priority: 1},
			linkOpts{id: 2, delayMS: slowMS, mbps: 600, priority: 2},
		)
	}

	// Bandwidth asymmetry.
	for i, fastMbps := range []float64{100, 300, 900} {
		add(fmt.Sprintf("bandwidth-asym-%d", i+1),
			fmt.Sprintf("fast %.0f Mbps plus 40 Mbps companion", fastMbps), 0.80,
			linkOpts{id: 1, delayMS: 8, mbps: fastMbps, priority: 1},
			linkOpts{id: 2, delayMS: 12, mbps: 40, priority: 2},
		)
	}

	// Loss matrix, including very lossy paths.
	for i, loss := range []float64{0, 0.005, 0.02, 0.06, 0.12, 0.22} {
		add(fmt.Sprintf("loss-%03d", i),
			fmt.Sprintf("fast loss %.1f%% and independent companion", loss*100), 0.65,
			linkOpts{id: 1, delayMS: 7, mbps: 500, loss: loss, priority: 1},
			linkOpts{id: 2, delayMS: 9, mbps: 500, loss: loss * 0.7, priority: 2},
		)
	}

	// Frequent disconnect combinations.
	for i, loss := range []float64{0, 0.02, 0.08, 0.18} {
		add(fmt.Sprintf("flap-%d", i+1),
			"one path disconnects frequently; outstanding DSN data is redirected", 0.55,
			linkOpts{id: 1, delayMS: 6, mbps: 500, loss: loss * 0.4, priority: 1},
			linkOpts{id: 2, delayMS: 18, mbps: 300, loss: loss, priority: 2, flap: true},
		)
	}

	// Three and four path heterogeneous cases.
	hetero := [][]linkOpts{
		{
			{id: 1, delayMS: 5, mbps: 700, loss: 0.005, priority: 1},
			{id: 2, delayMS: 15, mbps: 300, loss: 0.02, priority: 2},
			{id: 3, delayMS: 50, mbps: 120, loss: 0.05, priority: 3},
		},
		{
			{id: 1, delayMS: 4, mbps: 900, priority: 1},
			{id: 2, delayMS: 40, mbps: 200, loss: 0.04, priority: 2},
			{id: 3, delayMS: 8, mbps: 400, loss: 0.01, priority: 3},
		},
		{
			{id: 1, delayMS: 8, mbps: 250, loss: 0.03, priority: 1},
			{id: 2, delayMS: 80, mbps: 100, loss: 0.10, priority: 2, flap: true},
			{id: 3, delayMS: 12, mbps: 250, loss: 0.03, priority: 3},
			{id: 4, delayMS: 20, mbps: 180, loss: 0.05, priority: 4},
		},
	}
	for i, ops := range hetero {
		add(fmt.Sprintf("hetero-%d", i+1), "three/four heterogeneous paths", 0.65, ops...)
	}

	// Priority refill and max-subflow configurations are exercised separately,
	// but these extra pairwise seeds bring matrix into several dozen cases.
	delays := []int{5, 10, 25, 50}
	losses := []float64{0.002, 0.015, 0.05, 0.12}
	bws := []float64{120, 350, 800}
	k := 0
	for _, d := range delays {
		for _, l := range losses {
			for _, bw := range bws {
				if k >= 24 {
					break
				}
				k++
				add(fmt.Sprintf("combo-%02d", k),
					fmt.Sprintf("delay=%dms loss=%.1f%% bw=%.0fMbps", d, l*100, bw), 0.65,
					linkOpts{id: 1, delayMS: 6, mbps: bw, loss: l * 0.5, priority: 1},
					linkOpts{id: 2, delayMS: d, mbps: bw * 0.6, loss: l, priority: 2},
				)
			}
		}
	}
	return out
}

// BestSingle returns the profile expected to provide the best standalone
// goodput: low delay/loss and high rate. The benchmark still measures it.
func BestSingle(ps []simnet.Profile) simnet.Profile {
	best := ps[0]
	score := func(p simnet.Profile) float64 {
		// Conservative theoretical score: rate*(1-loss)/(RTT in seconds),
		// dominated in practice by measured baseline.
		rtt := 2 * p.OneWayDelay.Seconds()
		if rtt <= 0 {
			rtt = 0.001
		}
		return p.RateBytesPerSec * (1 - p.LossRate) / (rtt * 1000)
	}
	for _, p := range ps[1:] {
		if score(p) > score(best) {
			best = p
		}
	}
	return best
}
