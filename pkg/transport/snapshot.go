package transport

import (
	"fmt"
	"sort"
	"time"

	"clocktower/multipath/pkg/cc"
)

func (c *Conn) Snapshot() *Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	pathIDs := c.sortedPathIDsLocked()
	paths := make([]PathSnapshot, 0, len(pathIDs))
	for _, id := range pathIDs {
		p := c.paths[id]
		ps, ok := c.cc.Path(id)
		var w, ss float64
		if ok {
			w, ss = ps.W, ps.Ssthresh
		}
		paths = append(paths, PathSnapshot{
			PathID: p.pathID, SubflowID: p.id, Status: p.status, Priority: p.priority,
			Name: p.name, RateBps: p.profile.RateBytesPerSec, BaseDelay: p.profile.OneWayDelay,
			LossRate: p.profile.LossRate, RTT: rttOrInitial(p, c.cfg.InitialRTT),
			CWND: w, CWNDBytes: c.cc.WindowBytes(id), Ssthresh: ss,
			SubflightBytes: p.subFlight, SSNNext: p.nextSSN, SSNCumACK: p.cumSSNACK,
			SentBytes: p.sentBytes, AckedBytes: p.ackedBytes, SubACKs: p.subACKs,
			Retransmissions: p.retrans, Reinjections: p.reinject,
			DownEvents: p.downEvents, Rejoins: p.rejoins, RoundRobinTurns: p.rrTurns,
		})
	}

	queued := make([]AddressSnapshot, 0)
	for _, a := range c.addresses {
		if a.PathID == c.primaryID {
			continue
		}
		if p := c.paths[a.PathID]; p == nil {
			name := ""
			rate := 0.0
			if a.Link != nil {
				pr := a.Link.Profile()
				name = pr.Name
				rate = pr.RateBytesPerSec
			}
			_ = rate
			queued = append(queued, AddressSnapshot{PathID: a.PathID, Priority: a.Priority, Name: name})
		}
	}

	maps := append([]MappingRecord(nil), c.mappings...)
	if len(maps) > 120 {
		maps = maps[len(maps)-120:]
	}
	events := append([]EventRecord(nil), c.eventLog...)
	if len(events) > 80 {
		events = events[len(events)-80:]
	}

	pendingIDs := make([]uint64, 0, len(c.rcvPending))
	for dsn := range c.rcvPending {
		pendingIDs = append(pendingIDs, dsn)
	}
	sort.Slice(pendingIDs, func(i, j int) bool { return pendingIDs[i] < pendingIDs[j] })
	segs := make([]ReorderSegment, 0, min(120, len(pendingIDs)))
	for _, dsn := range pendingIDs {
		s := c.rcvPending[dsn]
		if len(segs) >= 120 {
			break
		}
		segs = append(segs, ReorderSegment{DSNStart: s.dsn, DSNEnd: s.end, Length: len(s.data)})
	}

	lats := append([]time.Duration(nil), c.latencies...)
	var avg, p95 float64
	if len(lats) > 0 {
		var sum time.Duration
		for _, d := range lats {
			sum += d
		}
		avg = float64(sum.Milliseconds()) / float64(len(lats))
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		idx := int(float64(len(lats)-1) * 0.95)
		p95 = float64(lats[idx].Microseconds()) / 1000.0
	}

	steps := c.cc.Steps()
	if len(steps) > 80 {
		steps = steps[len(steps)-80:]
	}

	active := 0
	for _, p := range c.paths {
		if p.status != statusClosed {
			active++
		}
	}

	return &Snapshot{
		Connected: c.established && !c.closed, Role: c.role, Scheduler: c.cfg.Scheduler,
		Mode: c.modeLocked(), ConnID: fmt.Sprintf("%016x", c.connID),
		MaxSubflows: c.cfg.MaxSubflows, ActiveSubflows: active, QueuedAddrs: len(queued),
		DSNUna: c.sndUna, DSNNext: c.sndNext, ConnFlight: c.connFlight,
		RemoteRWND: c.remoteRWND, LocalRWND: c.localRWND, SendBuffer: c.sendBuffer,
		TotalSent: c.totalSent, TotalAcked: c.totalAcked, TotalReceived: c.totalReceived,
		TotalDelivered: c.totalDelivered, Retransmissions: c.retransTotal,
		Reinjections: c.reinjectTotal, RescueEvents: c.rescueEvents,
		FallbackReason: c.fallbackReason, Alpha: c.cc.Alpha(),
		ReferenceCWND: c.reference.W, LatencyAvgMS: avg, LatencyP95MS: p95,
		StartedAt: c.startedAt, Paths: paths, QueuedAddresses: queued,
		Mappings: maps, Reorder: ReorderSnapshot{
			NextDSN: c.rcvNext, FinDSN: c.finDSN, HasFIN: c.remoteFIN,
			Buffered: c.storedBytes, Capacity: c.cfg.ReceiveBuffer,
			ReadyBytes: c.readyBytes, PendingBytes: c.pendingBytes,
			SegmentCount: len(c.rcvPending), Segments: segs,
			Gaps: c.gapsSnapshotLocked(40),
		},
		Events: events, FormulaSteps: []cc.Step(steps),
	}
}

func (c *Conn) gapsSnapshotLocked(limit int) []wireRangeJSON {
	out := make([]wireRangeJSON, 0)
	cursor := c.rcvNext
	for _, iv := range c.rcvIntervals.iv {
		if iv.end <= cursor {
			continue
		}
		if iv.start > cursor {
			out = append(out, wireRangeJSON{Start: cursor, End: iv.start})
			if len(out) >= limit {
				return out
			}
		}
		if iv.end > cursor {
			cursor = iv.end
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
