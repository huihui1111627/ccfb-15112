package transport

import (
	"sort"
	"time"

	"clocktower/multipath/pkg/wire"
)

// Write accepts application bytes, assigns shared DSNs and queues segments for
// scheduling. It applies backpressure from the local send retention budget.
func (c *Conn) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		c.mu.Lock()
		for !c.closed && c.sendBuffer >= c.cfg.SendBuffer {
			c.writeCond.Wait()
		}
		if c.closed {
			err := c.closeErr
			if err == nil {
				err = ErrClosed
			}
			c.mu.Unlock()
			return written, err
		}
		n := c.cfg.MSS
		if n > len(p) {
			n = len(p)
		}
		// Never exceed most of the retention budget in one segment.
		if maxN := c.cfg.SendBuffer - c.sendBuffer; n > maxN && maxN > 0 {
			n = maxN
		}
		if n <= 0 {
			n = 1
		}
		data := make([]byte, n)
		copy(data, p[:n])
		dsn := c.sndNext
		seg := &segment{dsn: dsn, length: n, data: data, txs: make(map[uint64]*txState)}
		c.segs[dsn] = seg
		c.sndNext += uint64(n)
		c.sendBuffer += n
		p = p[n:]
		written += n
		c.workCond.Broadcast()
		c.mu.Unlock()
	}
	return written, nil
}

func (c *Conn) sendLoop() {
	c.mu.Lock()
	for {
		if c.closed {
			c.mu.Unlock()
			return
		}
		if !c.established {
			c.workCond.Wait()
			continue
		}
		out := c.planLocked(32)
		if len(out) == 0 {
			c.workCond.Wait()
			continue
		}
		c.mu.Unlock()
		c.sendOutbound(out)
		c.mu.Lock()
	}
}

func (c *Conn) timerLoop() {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			c.mu.Lock()
			c.expireLostLocked()
			joinOut := c.retryJoinsLocked()
			c.maybeRetransmitFINLocked()
			c.workCond.Broadcast()
			progress := c.progressLocked()
			fn := c.progressFn
			c.mu.Unlock()
			if len(joinOut) > 0 {
				c.sendOutbound(joinOut)
			}
			if fn != nil && progress != nil {
				fn(*progress)
			}
		}
	}
}

func (c *Conn) progressLocked() *Progress {
	if c.progressFn == nil || time.Since(c.lastProgress) < 80*time.Millisecond {
		return nil
	}
	c.lastProgress = time.Now()
	complete := c.totalAcked
	elapsed := time.Since(c.startedAt).Seconds()
	var rate float64
	if elapsed > 0 {
		rate = float64(complete) / elapsed
	}
	return &Progress{
		BytesComplete:  complete,
		ThroughputBps:  rate,
		ActiveSubflows: c.openSubflowCountLocked(),
		Mode:           c.modeLocked(),
	}
}

func (c *Conn) modeLocked() string {
	if c.fallback {
		return "single-path-fallback"
	}
	return "multipath"
}

func (c *Conn) planLocked(maxPackets int) []outbound {
	out := make([]outbound, 0, maxPackets)
	for i := 0; i < maxPackets; i++ {
		o := c.planOneLocked()
		if o == nil {
			break
		}
		out = append(out, *o)
	}
	return out
}

func (c *Conn) planOneLocked() *outbound {
	if c.closed || !c.established || c.macKey == nil {
		return nil
	}

	// 1. Reliability first: resend data that a timer or link event marked lost.
	// While a known retry is pending but has no cwnd slot, do not let new data
	// occupy the slot first; otherwise old SSN holes can starve indefinitely.
	if c.hasPendingRetransmissionLocked() {
		if o := c.planRetransmissionLocked(); o != nil {
			return o
		}
		return nil
	}

	// 2. Send new DSN data according to the selected scheduler.
	if o := c.planNewDataLocked(); o != nil {
		return o
	}

	// 3. If the shared connection window is blocked behind a slow path, use a
	// fast path to reinject its still-conn-unacknowledged data.
	if o := c.planSlowPathRescueLocked(); o != nil {
		return o
	}

	// 4. FIN is scheduled only after all data is connection-acknowledged.
	if o := c.planFINLocked(); o != nil {
		return o
	}
	return nil
}

func (c *Conn) sortedPathIDsLocked() []uint16 {
	ids := make([]uint16, 0, len(c.paths))
	for id := range c.paths {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (c *Conn) pathEligibleLocked(p *subflow, length int) bool {
	if p == nil || p.status != statusActive {
		return false
	}
	if c.fallback && !p.isPrimary {
		return false
	}
	cwnd := c.cc.WindowBytes(p.pathID)
	return cwnd-int(p.subFlight) >= length
}

func (c *Conn) selectScheduledPathLocked(length int) *subflow {
	ids := c.sortedPathIDsLocked()
	if c.fallback || c.cfg.Scheduler == SchedulerSingle {
		return c.paths[c.primaryID]
	}
	switch c.cfg.Scheduler {
	case SchedulerRoundRobin:
		n := len(ids)
		for k := 0; k < n; k++ {
			idx := (c.rrCursor + k) % n
			p := c.paths[ids[idx]]
			if c.pathEligibleLocked(p, length) {
				c.rrCursor = (idx + 1) % n
				p.rrTurns++
				return p
			}
		}
		return nil
	case SchedulerMinRTT:
		var best *subflow
		bestRTT := time.Duration(1<<63 - 1)
		for _, id := range ids {
			p := c.paths[id]
			if !c.pathEligibleLocked(p, length) {
				continue
			}
			rtt := rttOrInitial(p, c.cfg.InitialRTT)
			if rtt < bestRTT {
				bestRTT = rtt
				best = p
			}
		}
		return best
	default:
		return c.selectScheduledPathLockedWith(length, SchedulerMinRTT)
	}
}

func (c *Conn) selectScheduledPathLockedWith(length int, kind SchedulerKind) *subflow {
	old := c.cfg.Scheduler
	c.cfg.Scheduler = kind
	p := c.selectScheduledPathLocked(length)
	c.cfg.Scheduler = old
	return p
}

func (c *Conn) preferredRetransPathLocked(seg *segment) *subflow {
	// Prefer the most recent transmission's path when it is active. Reusing
	// that SSN closes the subflow reorder hole and keeps its ACK clock moving.
	if p := c.paths[seg.lastTxPath]; p != nil && c.pathEligibleLocked(p, seg.length) {
		return p
	}
	// Otherwise try any active path that already has an SSN mapping.
	for _, tx := range seg.txs {
		p := c.paths[tx.pathID]
		if p != nil && p.status == statusActive && c.pathEligibleLocked(p, seg.length) {
			return p
		}
	}
	return nil
}

func (c *Conn) fastestPathLocked(length int) *subflow {
	var best *subflow
	bestRTT := time.Duration(1<<63 - 1)
	for _, p := range c.paths {
		if !c.pathEligibleLocked(p, length) {
			continue
		}
		rtt := rttOrInitial(p, c.cfg.InitialRTT)
		if rtt < bestRTT {
			bestRTT = rtt
			best = p
		}
	}
	return best
}

func (c *Conn) hasPendingRetransmissionLocked() bool {
	for _, seg := range c.segs {
		if !seg.connAcked && seg.needsRetrans {
			return true
		}
	}
	return false
}

func (c *Conn) planRetransmissionLocked() *outbound {
	var oldest *segment
	for _, seg := range c.segs {
		if seg.connAcked || !seg.needsRetrans {
			continue
		}
		if c.hasLiveTxLocked(seg) {
			seg.needsRetrans = false
			continue
		}
		if oldest == nil || seg.dsn < oldest.dsn {
			oldest = seg
		}
	}
	if oldest == nil {
		return nil
	}
	// An active original path first reuses its old SSN mapping to fill its
	// subflow sequence hole. If that path is down or has no window, redirect
	// the DSN data to the fastest usable subflow with a fresh SSN mapping.
	p := c.preferredRetransPathLocked(oldest)
	if p == nil {
		p = c.fastestPathLocked(oldest.length)
	}
	if p == nil {
		p = c.selectScheduledPathLocked(oldest.length)
	}
	if p == nil {
		return nil
	}
	oldest.needsRetrans = false
	return c.transmitSegmentLocked(oldest, p, wire.FlagRetransmission, "timeout/loss retransmission", true, false)
}

func (c *Conn) hasLiveTxLocked(seg *segment) bool {
	for _, tx := range seg.txs {
		if !tx.subAcked && !tx.lost {
			return true
		}
	}
	return false
}

func (c *Conn) planNewDataLocked() *outbound {
	if c.sndSendNext >= c.sndNext {
		return nil
	}
	seg := c.segs[c.sndSendNext]
	if seg == nil || seg.connAcked || seg.data == nil {
		// Should not happen, but recover the cursor rather than spinning.
		c.sndSendNext += uint64(c.cfg.MSS)
		return nil
	}
	end := seg.dsn + uint64(seg.length)
	if end > c.remoteWindowEdge {
		return nil
	}
	p := c.selectScheduledPathLocked(seg.length)
	if p == nil {
		return nil
	}
	c.sndSendNext = end
	return c.transmitSegmentLocked(seg, p, 0, "new data", false, false)
}

func (c *Conn) connectionWindowBlockedLocked() bool {
	if c.sndSendNext >= c.sndNext {
		return false
	}
	seg := c.segs[c.sndSendNext]
	if seg == nil {
		return false
	}
	return seg.dsn+uint64(seg.length) > c.remoteWindowEdge
}

func (c *Conn) planSlowPathRescueLocked() *outbound {
	if c.fallback || !c.connectionWindowBlockedLocked() {
		return nil
	}
	fast, slow := c.rescuePathsLocked()
	if fast == nil || slow == nil {
		return nil
	}

	// Find the oldest connection-unacknowledged segment carried by slow.
	var candidate *segment
	for _, seg := range c.segs {
		if seg.connAcked || seg.data == nil || seg.rescueEpoch == c.rescueEpoch && c.rescueEpoch != 0 {
			continue
		}
		onSlow := false
		for _, tx := range seg.txs {
			if tx.pathID == slow.pathID {
				onSlow = true
				break
			}
		}
		if !onSlow {
			continue
		}
		if candidate == nil || seg.dsn < candidate.dsn {
			candidate = seg
		}
	}
	if candidate == nil || !c.pathEligibleLocked(fast, candidate.length) {
		return nil
	}

	// Throttle multiplicative decreases to at most once per roughly one fast
	// RTT. Within an epoch all outstanding slow data is reinjected as cwnd
	// permits.
	fastRTT := rttOrInitial(fast, c.cfg.InitialRTT)
	if fastRTT < 20*time.Millisecond {
		fastRTT = 20 * time.Millisecond
	}
	if c.rescueEpoch == 0 || time.Since(c.lastRescue) > fastRTT {
		c.rescueEpoch++
		c.rescueEvents++
		c.lastRescue = time.Now()
		c.cc.Halve(slow.pathID, "slow path filled shared connection window")
		c.addEventLocked("warn",
			"shared window blocked behind slow path %d (rtt=%s); fast path %d reinjects and halves cwnd",
			slow.pathID, rttOrInitial(slow, c.cfg.InitialRTT), fast.pathID)
	}
	if candidate.rescueEpoch == c.rescueEpoch {
		return nil
	}
	candidate.rescueEpoch = c.rescueEpoch
	return c.transmitSegmentLocked(candidate, fast,
		wire.FlagRedundant|wire.FlagRetransmission, "fast-path rescue reinjection", true, true)
}

func (c *Conn) rescuePathsLocked() (fast *subflow, slow *subflow) {
	fastRTT := time.Duration(1<<63 - 1)
	slowRTT := time.Duration(0)
	for _, p := range c.paths {
		if p.status != statusActive || p.isPrimary && c.fallback {
			continue
		}
		rtt := rttOrInitial(p, c.cfg.InitialRTT)
		if rtt < fastRTT {
			fastRTT = rtt
			fast = p
		}
		if rtt > slowRTT {
			slowRTT = rtt
			slow = p
		}
	}
	if fast == nil || slow == nil || fast.pathID == slow.pathID {
		return nil, nil
	}
	// Ten-fold RTT differences are the main case; a modest ratio also catches
	// a clearly lagging path without reacting to rounding noise.
	if slowRTT < fastRTT*3/2 {
		return nil, nil
	}
	return fast, slow
}

func (c *Conn) transmitSegmentLocked(seg *segment, p *subflow, flags uint8, reason string, retrans, redundant bool) *outbound {
	now := time.Now()
	var tx *txState

	// Retransmission on the same subflow reuses its SSN mapping. A different
	// subflow receives a fresh SSN and a new DSN<->SSN mapping.
	for _, existing := range seg.txs {
		if existing.pathID == p.pathID && !existing.subAcked {
			tx = existing
			break
		}
	}

	ssn := uint64(0)
	subflowID := p.id
	if tx != nil {
		ssn = tx.ssnStart
		tx.sentAt = now
		tx.lost = false
		tx.rttSampleOK = false // reused SSN makes the RTT sample ambiguous
		tx.isRetrans = retrans
		tx.isRedundant = redundant
		p.subFlight += int64(seg.length)
		p.txBySSN[ssn] = tx
	} else {
		ssn = p.nextSSN
		p.nextSSN += uint64(seg.length)
		c.txSeq++
		tx = &txState{
			attemptID:   c.txSeq,
			pathID:      p.pathID,
			subflowID:   p.id,
			ssnStart:    ssn,
			length:      seg.length,
			sentAt:      now,
			rttSampleOK: true,
			isRetrans:   retrans,
			isRedundant: redundant,
		}
		seg.txs[tx.attemptID] = tx
		p.txBySSN[ssn] = tx
		p.subFlight += int64(seg.length)
	}
	tx.accounted = true

	if !seg.sentOnce {
		seg.sentOnce = true
		c.connFlight += int64(seg.length)
	}
	p.sentBytes += int64(seg.length)
	c.totalSent += int64(seg.length)
	if retrans {
		p.retrans++
		c.retransTotal++
	}
	if redundant {
		p.reinject++
		c.reinjectTotal++
	}
	seg.lastTxPath = p.pathID

	m := wire.Mapping{
		PathID:   p.pathID,
		DSNStart: seg.dsn,
		DSNLen:   uint32(seg.length),
		SSNStart: ssn,
		SSNLen:   uint32(seg.length),
	}
	h := wire.Header{
		Flags:     flags,
		PathID:    p.pathID,
		SubflowID: subflowID,
		ConnID:    c.connID,
	}
	raw := wire.EncodeData(h, m, seg.data, c.macKey)
	c.addMappingLocked(MappingRecord{
		At: now, PathID: p.pathID, SubflowID: subflowID,
		DSNStart: seg.dsn, DSNEnd: seg.dsn + uint64(seg.length),
		SSNStart: ssn, SSNEnd: ssn + uint64(seg.length),
		Length: seg.length, Flags: flags, Reason: reason,
	})
	return &outbound{pathID: p.pathID, raw: raw}
}

func (c *Conn) expireLostLocked() {
	now := time.Now()
	lossPaths := map[uint16]bool{}
	for _, seg := range c.segs {
		if seg.connAcked {
			continue
		}
		for _, tx := range seg.txs {
			if tx.subAcked || tx.lost {
				continue
			}
			p := c.paths[tx.pathID]
			rto := c.cfg.InitialRTT * 2
			if p != nil {
				rto = p.rto()
			}
			if now.Sub(tx.sentAt) > rto {
				tx.lost = true
				tx.rttSampleOK = false
				if p != nil && tx.accounted {
					p.subFlight -= int64(tx.length)
					tx.accounted = false
				}
				seg.needsRetrans = true
				lossPaths[tx.pathID] = true
			}
		}
	}
	for id := range lossPaths {
		c.cc.Timeout(id, "retransmission timeout")
	}
}

func (c *Conn) markPathDownLocked(pathID uint16) {
	for _, seg := range c.segs {
		if seg.connAcked {
			continue
		}
		for _, tx := range seg.txs {
			if tx.pathID != pathID || tx.subAcked || tx.lost {
				continue
			}
			tx.lost = true
			tx.rttSampleOK = false
			if p := c.paths[pathID]; p != nil && tx.accounted {
				p.subFlight -= int64(tx.length)
				tx.accounted = false
			}
			seg.needsRetrans = true
		}
	}
}
