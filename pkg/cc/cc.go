// Package cc implements the linked, TCP-friendly congestion controller used
// across subflows.
//
// The congestion-avoidance update is the Linked Increases Algorithm:
//
//	alpha = W_total * max_i(w_i / rtt_i^2) / (sum_i(w_i / rtt_i))^2
//	for each ACK on subflow r:
//	    delta = min(alpha / W_total, 1 / w_r)
//	    w_r  += delta
//
// During slow start w_r increases by one MSS per acknowledged MSS. Loss and
// explicit rescue events halve the affected window. Every mutation returns a
// Step so tests and the dashboard can replay the exact arithmetic.
package cc

import (
	"math"
	"sort"
	"time"
)

const (
	DefaultMSS                = 16 * 1024
	DefaultInitialWindow      = 10.0
	DefaultSlowStartThreshold = 1000.0
	MinWindow                 = 1.0
)

type PathState struct {
	ID       uint16        `json:"id"`
	W        float64       `json:"cwnd_mss"`
	Ssthresh float64       `json:"ssthresh_mss"`
	RTT      time.Duration `json:"rtt_ns"`
}

type Step struct {
	Kind           string  `json:"kind"`
	PathID         uint16  `json:"path_id"`
	Before         float64 `json:"before"`
	After          float64 `json:"after"`
	Increment      float64 `json:"increment"`
	Alpha          float64 `json:"alpha"`
	AlphaOverTotal float64 `json:"alpha_over_total"`
	TotalBefore    float64 `json:"total_before"`
	Reason         string  `json:"reason,omitempty"`
}

type Group struct {
	mss       int
	initialW  float64
	initialSS float64
	paths     map[uint16]*PathState
	order     []uint16
	lastAlpha float64
	steps     []Step
}

func NewGroup(mss int) *Group {
	if mss <= 0 {
		mss = DefaultMSS
	}
	return &Group{
		mss:       mss,
		initialW:  DefaultInitialWindow,
		initialSS: DefaultSlowStartThreshold,
		paths:     make(map[uint16]*PathState),
	}
}

func (g *Group) SetInitialWindow(w float64) {
	if w >= MinWindow {
		g.initialW = w
	}
}

func (g *Group) MSS() int { return g.mss }

func (g *Group) AddPath(id uint16, rtt time.Duration) *PathState {
	if p, ok := g.paths[id]; ok {
		p.RTT = rtt
		return p
	}
	p := &PathState{ID: id, W: g.initialW, Ssthresh: g.initialSS, RTT: rtt}
	g.paths[id] = p
	g.order = append(g.order, id)
	sort.Slice(g.order, func(i, j int) bool { return g.order[i] < g.order[j] })
	return p
}

func (g *Group) RemovePath(id uint16) {
	delete(g.paths, id)
	for i, v := range g.order {
		if v == id {
			g.order = append(g.order[:i], g.order[i+1:]...)
			break
		}
	}
}

func (g *Group) Path(id uint16) (PathState, bool) {
	p, ok := g.paths[id]
	if !ok {
		return PathState{}, false
	}
	return *p, true
}

func (g *Group) Snapshot() []PathState {
	out := make([]PathState, 0, len(g.order))
	for _, id := range g.order {
		out = append(out, *g.paths[id])
	}
	return out
}

func (g *Group) SetRTT(id uint16, rtt time.Duration) {
	if p, ok := g.paths[id]; ok && rtt > 0 {
		// Exponential moving average is applied by the caller; this setter is
		// deliberately last-write for test-driven formula replay.
		p.RTT = rtt
	}
}

func (g *Group) TotalWindowMSS() float64 {
	total := 0.0
	for _, p := range g.paths {
		total += p.W
	}
	return total
}

func (g *Group) Alpha() float64 {
	total := g.TotalWindowMSS()
	if total <= 0 || len(g.paths) == 0 {
		g.lastAlpha = 1
		return 1
	}
	var sumWRtt, maxWRtt2 float64
	for _, id := range g.order {
		p := g.paths[id]
		rtt := p.RTT.Seconds()
		if rtt <= 0 {
			rtt = 0.1
		}
		sumWRtt += p.W / rtt
		if v := p.W / (rtt * rtt); v > maxWRtt2 {
			maxWRtt2 = v
		}
	}
	if sumWRtt == 0 {
		g.lastAlpha = 1
		return 1
	}
	alpha := total * maxWRtt2 / (sumWRtt * sumWRtt)
	if math.IsNaN(alpha) || math.IsInf(alpha, 0) || alpha <= 0 {
		alpha = 1
	}
	g.lastAlpha = alpha
	return alpha
}

func (g *Group) LastAlpha() float64 { return g.lastAlpha }

// AckOne applies one ACK/one newly acknowledged MSS to path id and records the
// formula step.
func (g *Group) AckOne(id uint16) Step {
	p := g.paths[id]
	if p == nil {
		p = g.AddPath(id, 0)
	}
	before := p.W
	totalBefore := g.TotalWindowMSS()
	st := Step{Kind: "ack", PathID: id, Before: before, TotalBefore: totalBefore}

	if p.W < p.Ssthresh {
		st.Increment = 1
		st.Reason = "slow-start"
		p.W = before + 1
	} else {
		alpha := g.Alpha()
		st.Alpha = alpha
		st.AlphaOverTotal = alpha / totalBefore
		st.Increment = math.Min(alpha/totalBefore, 1.0/before)
		if st.Increment < 0 {
			st.Increment = 0
		}
		p.W = before + st.Increment
		st.Reason = "linked-congestion-avoidance"
	}
	st.After = p.W
	g.steps = append(g.steps, st)
	if len(g.steps) > 2048 {
		g.steps = g.steps[len(g.steps)-2048:]
	}
	return st
}

// Loss halves the path window and records the multiplicative decrease.
func (g *Group) Loss(id uint16, reason string) Step {
	p := g.paths[id]
	if p == nil {
		p = g.AddPath(id, 0)
	}
	before := p.W
	p.Ssthresh = math.Max(MinWindow*2, before/2)
	p.W = math.Max(MinWindow, before/2)
	st := Step{
		Kind: "loss", PathID: id, Before: before, After: p.W,
		Increment: p.W - before, TotalBefore: g.TotalWindowMSS(), Reason: reason,
	}
	g.steps = append(g.steps, st)
	return st
}

// Timeout applies Reno-style timeout recovery: ssthresh is halved and cwnd
// returns to one MSS, after which slow start can run back to ssthresh.
func (g *Group) Timeout(id uint16, reason string) Step {
	p := g.paths[id]
	if p == nil {
		p = g.AddPath(id, 0)
	}
	before := p.W
	p.Ssthresh = math.Max(MinWindow*2, before/2)
	p.W = MinWindow
	st := Step{
		Kind: "timeout", PathID: id, Before: before, After: p.W,
		Increment: p.W - before, TotalBefore: g.TotalWindowMSS(), Reason: reason,
	}
	g.steps = append(g.steps, st)
	return st
}

// Halve is an explicit linked correction used when a fast path rescues data
// that has filled the shared window behind a slow path.
func (g *Group) Halve(id uint16, reason string) Step {
	st := g.Loss(id, reason)
	st.Kind = "halve"
	return st
}

func (g *Group) WindowBytes(id uint16) int {
	p, ok := g.paths[id]
	if !ok {
		return g.mss
	}
	n := int(math.Floor(p.W * float64(g.mss)))
	if n < g.mss {
		n = g.mss
	}
	return n
}

func (g *Group) Steps() []Step {
	out := make([]Step, len(g.steps))
	copy(out, g.steps)
	return out
}

// OrdinaryReno is the single-path reference used to make the friendliness
// bound visible.
type OrdinaryReno struct {
	W        float64 `json:"cwnd_mss"`
	Ssthresh float64 `json:"ssthresh_mss"`
}

func NewOrdinaryReno() OrdinaryReno {
	return OrdinaryReno{W: DefaultInitialWindow, Ssthresh: DefaultSlowStartThreshold}
}

func (r *OrdinaryReno) AckOne() float64 {
	before := r.W
	if r.W < r.Ssthresh {
		r.W++
	} else {
		r.W += 1 / r.W
	}
	return r.W - before
}

func (r *OrdinaryReno) Loss() {
	r.Ssthresh = math.Max(2, r.W/2)
	r.W = math.Max(1, r.W/2)
}
