package transport

import (
	"errors"
	"io"
	"time"

	"clocktower/multipath/pkg/wire"
)

// Read returns the in-order shared DSN byte stream. Out-of-order segments stay
// in the reorder buffer until missing data arrives.
func (c *Conn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.ready) == 0 && !c.closed {
		if c.remoteFIN && c.rcvNext >= c.finDSN {
			return 0, io.EOF
		}
		c.readCond.Wait()
	}
	if c.closed {
		return 0, c.closeErr
	}
	if len(c.ready) == 0 {
		if c.remoteFIN && c.rcvNext >= c.finDSN {
			return 0, io.EOF
		}
		return 0, ErrClosed
	}

	n := 0
	for n < len(p) && len(c.ready) > 0 {
		head := c.ready[0]
		taken := copy(p[n:], head)
		n += taken
		c.totalDelivered += int64(taken)
		c.readyBytes -= taken
		c.storedBytes -= taken
		if taken == len(head) {
			c.ready = c.ready[1:]
		} else {
			c.ready[0] = head[taken:]
		}
	}
	c.localRWND = c.currentRWNDLocked()
	c.connACKPending = true
	c.workCond.Broadcast()
	return n, nil
}

func (c *Conn) currentRWNDLocked() uint32 {
	cap := int(c.cfg.ReceiveBuffer)
	if c.storedBytes > cap {
		return 0
	}
	return uint32(cap - c.storedBytes)
}

func (c *Conn) HandlePacket(pathID uint16, pkt []byte) []outbound {
	h, err := wire.DecodeHeader(pkt)
	if err != nil {
		// A split fragment can lose the fixed header. The first fragment still
		// reaches DATA mapping validation and triggers authenticated fallback.
		return nil
	}

	switch h.Type {
	case wire.TypeJOIN:
		return c.handleJOIN(pkt)
	case wire.TypeJOINACK:
		return c.handleJOINACK(pkt)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	switch h.Type {
	case wire.TypeData:
		return c.handleDataLocked(pathID, pkt)
	case wire.TypeSubflowACK:
		c.handleSubflowACKLocked(pkt)
		return nil
	case wire.TypeConnectionACK:
		c.handleConnectionACKLocked(pkt)
		return nil
	case wire.TypeFIN:
		return c.handleFINLocked(pkt)
	case wire.TypeFINACK:
		c.finAcked = true
		c.writeCond.Broadcast()
		return nil
	case wire.TypeSubflowClose:
		if p := c.paths[h.PathID]; p != nil && !p.isPrimary {
			p.status = statusClosed
			c.addEventLocked("warn", "peer closed subflow %d/path %d", h.SubflowID, h.PathID)
			c.workCond.Broadcast()
		}
		return nil
	case wire.TypeFallback:
		reason, err := wire.DecodeFallback(pkt, c.macKey)
		if err == nil {
			c.enableFallbackLocked("peer: "+fallbackReason(reason), false)
		}
		return nil
	default:
		return nil
	}
}

func fallbackReason(r uint8) string {
	if r == 1 {
		return "middlebox mapping/option failure"
	}
	return "mapping verification failure"
}

func (c *Conn) handleDataLocked(recvPath uint16, pkt []byte) []outbound {
	h, m, payload, err := wire.DecodeData(pkt, c.macKey)
	if err != nil {
		if errors.Is(err, wire.ErrMappingCheck) || errors.Is(err, wire.ErrAuth) {
			return c.fallbackFromMappingFailureLocked(recvPath, err)
		}
		return nil
	}
	if h.ConnID != c.connID {
		return nil
	}
	if !c.established {
		// A keyed DATA mapping is also key confirmation if the final HACK was
		// lost on a lossy path.
		c.establishLocked()
	}
	pathID := m.PathID
	p := c.paths[pathID]
	if p == nil {
		// A server can receive DATA immediately after creating JOIN state;
		// create a minimal receiver if an address/link exists.
		a, ok := c.addressByPathLocked(pathID)
		if !ok {
			return nil
		}
		p = newSubflow(a, h.SubflowID, pathID == c.primaryID, c.cfg.InitialRTT)
		p.status = statusActive
		c.paths[pathID] = p
		c.cc.AddPath(pathID, p.srtt)
	}

	// Independent subflow receive interval and SUB-ACK.
	ssnEnd := m.SSNEnd()
	if !p.recvRanges.covers(m.SSNStart, ssnEnd) {
		p.recvRanges.add(m.SSNStart, ssnEnd)
		advanceSubflowReceive(p)
	}
	// Always put the just-received SSN range first. During long-lived holes
	// the interval set's earliest SACK blocks can otherwise crowd out the
	// newest feedback; this exact block keeps the subflow cwnd ACK clock
	// independent of the stuck cumulative SSN.
	sacks := []wire.Range{{Start: m.SSNStart, End: ssnEnd}}
	sacks = append(sacks, p.recvRanges.sack(p.recvNextSSN, 31)...)
	subAck := wire.EncodeSubflowACK(c.connID, pathID, p.id, p.recvNextSSN, sacks)
	out := []outbound{{pathID: pathID, raw: subAck}}

	// Shared connection reorder buffer.
	dsnEnd := m.DSNEnd()
	if c.rcvIntervals.covers(m.DSNStart, dsnEnd) {
		return out
	}
	if c.storedBytes+len(payload) > c.cfg.ReceiveBuffer {
		c.localRWND = 0
		return out
	}
	data := append([]byte(nil), payload...)
	c.rcvPending[m.DSNStart] = &reorderSegment{dsn: m.DSNStart, end: dsnEnd, data: data}
	c.pendingBytes += len(data)
	c.storedBytes += len(data)
	c.totalReceived += int64(len(data))
	c.rcvIntervals.add(m.DSNStart, dsnEnd)

	for {
		seg, ok := c.rcvPending[c.rcvNext]
		if !ok {
			break
		}
		delete(c.rcvPending, c.rcvNext)
		c.rcvNext = seg.end
		c.pendingBytes -= len(seg.data)
		c.ready = append(c.ready, seg.data)
		c.readyBytes += len(seg.data)
	}
	c.localRWND = c.currentRWNDLocked()
	c.connACKPending = true
	if c.ackPathID == 0 {
		c.ackPathID = recvPath
	} else {
		c.ackPathID = recvPath
	}
	c.readCond.Broadcast()
	c.workCond.Broadcast()

	if c.remoteFIN && c.rcvNext >= c.finDSN {
		c.readCond.Broadcast()
		finack := wire.EncodeSimple(wire.TypeFINACK, c.connID, c.primaryID, 0, nil)
		out = append(out, outbound{pathID: c.primaryID, raw: finack})
	}
	return out
}

func advanceSubflowReceive(p *subflow) {
	for {
		advanced := false
		for _, iv := range p.recvRanges.iv {
			if iv.start <= p.recvNextSSN && p.recvNextSSN < iv.end {
				p.recvNextSSN = iv.end
				advanced = true
				break
			}
			if iv.start > p.recvNextSSN {
				break
			}
		}
		if !advanced {
			return
		}
	}
}

func (c *Conn) addressByPathLocked(pathID uint16) (Address, bool) {
	for _, a := range c.addresses {
		if a.PathID == pathID {
			return a, true
		}
	}
	if l, ok := c.links[pathID]; ok {
		return Address{PathID: pathID, Link: l}, true
	}
	return Address{}, false
}

func (c *Conn) fallbackFromMappingFailureLocked(pathID uint16, cause error) []outbound {
	raw := c.enableFallbackLocked("mapping check failed: "+cause.Error(), true)
	if raw == nil {
		return nil
	}
	// Notify over both the observed path and primary to survive loss.
	out := []outbound{{pathID: c.primaryID, raw: raw}}
	if pathID != c.primaryID {
		out = append(out, outbound{pathID: pathID, raw: raw})
	}
	return out
}

func (c *Conn) enableFallbackLocked(reason string, createPacket bool) []byte {
	if !c.fallback {
		c.fallback = true
		c.fallbackReason = reason
		c.cfg.Scheduler = SchedulerSingle
		c.addEventLocked("error", "falling back to ordinary single-path transfer: %s", reason)
		for id, p := range c.paths {
			if id == c.primaryID {
				p.status = statusActive
				continue
			}
			if p.status != statusClosed {
				p.status = statusClosed
				c.markPathDownLocked(id)
			}
		}
		c.workCond.Broadcast()
	}
	if createPacket && c.macKey != nil {
		return wire.EncodeFallback(c.connID, 1, c.macKey)
	}
	return nil
}

func (c *Conn) handleSubflowACKLocked(pkt []byte) {
	h, cum, ranges, err := wire.DecodeSubflowACK(pkt)
	if err != nil {
		return
	}
	p := c.paths[h.PathID]
	if p == nil {
		return
	}
	p.subACKs++
	if cum > p.cumSSNACK {
		p.cumSSNACK = cum
	}
	c.ackSSNRangeLocked(p, wire.Range{Start: 0, End: cum})
	for _, r := range ranges {
		c.ackSSNRangeLocked(p, r)
	}
	c.workCond.Broadcast()
}

func (c *Conn) ackSSNRangeLocked(p *subflow, r wire.Range) {
	if r.End <= r.Start {
		return
	}
	// Exact segment starts make SACK processing O(number acknowledged).
	pos := r.Start
	for pos < r.End {
		tx := p.txBySSN[pos]
		if tx == nil {
			// Cumulative ACK may start at zero; scan once for covered tx.
			if r.Start == 0 {
				var best *txState
				for ssn, candidate := range p.txBySSN {
					if !candidate.subAcked && candidate.ssnStart+uint64(candidate.length) <= r.End {
						if best == nil || ssn < best.ssnStart {
							best = candidate
						}
					}
				}
				tx = best
				if tx == nil {
					return
				}
				pos = tx.ssnStart
			} else {
				return
			}
		}
		end := pos + uint64(tx.length)
		if end > r.End && r.Start != 0 {
			return
		}
		c.ackOneTXLocked(p, tx)
		delete(p.txBySSN, pos)
		pos = end
	}
}

func (c *Conn) ackOneTXLocked(p *subflow, tx *txState) {
	if tx.subAcked {
		return
	}
	tx.subAcked = true
	if tx.accounted {
		p.subFlight -= int64(tx.length)
		tx.accounted = false
	}
	if p.subFlight < 0 {
		p.subFlight = 0
	}
	p.ackedBytes += int64(tx.length)
	if tx.rttSampleOK {
		updateRTT(p, time.Since(tx.sentAt))
	}
	c.cc.SetRTT(p.pathID, p.srtt)
	c.cc.AckOne(p.pathID)
}

func (c *Conn) handleConnectionACKLocked(pkt []byte) {
	_, cum, rwnd, ranges, err := wire.DecodeConnectionACK(pkt)
	if err != nil {
		return
	}
	c.remoteRWND = rwnd
	c.remoteWindowEdge = cum + uint64(rwnd)
	if cum > c.sndUna {
		c.sndUna = cum
	}

	// Cumulative ACK releases and removes all older segments.
	for dsn, seg := range c.segs {
		if dsn+uint64(seg.length) <= cum {
			c.releaseConnSegmentLocked(seg)
			delete(c.segs, dsn)
		}
	}
	// Selective connection ACKs release cache immediately even though a gap
	// prevents cumulative advancement.
	for _, r := range ranges {
		pos := r.Start
		for pos < r.End {
			seg := c.segs[pos]
			if seg == nil {
				break
			}
			end := pos + uint64(seg.length)
			if end > r.End {
				break
			}
			c.releaseConnSegmentLocked(seg)
			pos = end
		}
	}
	// SACK blocks release cache. We deliberately do not instant-retransmit
	// from one SACK: heterogeneous subflows legitimately reorder DSNs, and an
	// immediate gap verdict would falsely halve cwnd. RTO/link-down and the
	// explicit slow-path rescue handle loss.
	c.workCond.Broadcast()
	c.writeCond.Broadcast()
}

func (c *Conn) fastRetransmitFromSACKLocked(ranges []wire.Range) {
	lossPaths := map[uint16]bool{}
	for _, r := range ranges {
		dsn := c.sndUna
		for dsn < r.Start {
			seg := c.segs[dsn]
			if seg == nil {
				break
			}
			end := dsn + uint64(seg.length)
			if end > r.Start {
				break
			}
			if !seg.connAcked && seg.sentOnce {
				marked := false
				for _, tx := range seg.txs {
					if tx.subAcked || tx.lost {
						continue
					}
					tx.lost = true
					tx.rttSampleOK = false
					if tx.accounted {
						if p := c.paths[tx.pathID]; p != nil {
							p.subFlight -= int64(tx.length)
							if p.subFlight < 0 {
								p.subFlight = 0
							}
						}
						tx.accounted = false
					}
					lossPaths[tx.pathID] = true
					marked = true
				}
				if marked {
					seg.needsRetrans = true
				}
			}
			dsn = end
		}
	}
	for id := range lossPaths {
		c.cc.Loss(id, "connection SACK fast retransmit")
	}
}

func (c *Conn) releaseConnSegmentLocked(seg *segment) {
	if seg.connAcked {
		return
	}
	seg.connAcked = true
	// Connection ACK and subflow ACK remain independent protocol events. We
	// do not synthesize a subflow RTT/cwnd ACK here; we only remove retained
	// in-flight accounting so a lost SUB-ACK cannot leak the scheduler budget.
	c.settleTXAccountingLocked(seg)
	c.connFlight -= int64(seg.length)
	if c.connFlight < 0 {
		c.connFlight = 0
	}
	c.totalAcked += int64(seg.length)
	c.sendBuffer -= seg.length
	if c.sendBuffer < 0 {
		c.sendBuffer = 0
	}
	// Measure from the first original transmission when available.
	var first time.Time
	for _, tx := range seg.txs {
		if first.IsZero() || tx.sentAt.Before(first) {
			first = tx.sentAt
		}
	}
	if !first.IsZero() {
		c.recordLatency(time.Since(first))
	}
	c.reference.AckOne()
	seg.data = nil
	c.writeCond.Broadcast()
}

func (c *Conn) settleTXAccountingLocked(seg *segment) {
	for _, tx := range seg.txs {
		if tx.accounted {
			if p := c.paths[tx.pathID]; p != nil {
				p.subFlight -= int64(tx.length)
				if p.subFlight < 0 {
					p.subFlight = 0
				}
				// Keep txBySSN until an independent SUB-ACK arrives; that ACK
				// still performs the subflow cwnd update and deletes the entry.
			}
			tx.accounted = false
		}
	}
}

func (c *Conn) buildConnACKLocked() outbound {
	sacks := c.rcvIntervals.sack(c.rcvNext, 12)
	rwnd := c.currentRWNDLocked()
	raw := wire.EncodeConnectionACK(c.connID, c.rcvNext, rwnd, sacks)
	pathID := c.primaryID
	if p := c.paths[c.ackPathID]; p != nil && p.status == statusActive {
		pathID = c.ackPathID
	} else if p := c.paths[c.primaryID]; p != nil && p.status == statusActive {
		pathID = c.primaryID
	} else {
		// Pick any active path if primary is momentarily down.
		for id, p := range c.paths {
			if p.status == statusActive {
				pathID = id
				break
			}
		}
	}
	c.connACKPending = false
	c.lastConnACK = time.Now()
	return outbound{pathID: pathID, raw: raw}
}

func (c *Conn) ackLoop() {
	t := time.NewTicker(c.cfg.ACKDelay)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
			c.mu.Lock()
			if !c.established || c.macKey == nil {
				c.mu.Unlock()
				continue
			}
			o := c.buildConnACKLocked()
			c.mu.Unlock()
			c.sendOutbound([]outbound{o})
		}
	}
}

func (c *Conn) handleFINLocked(pkt []byte) []outbound {
	finalDSN, err := wire.DecodeFIN(pkt)
	if err != nil {
		return nil
	}
	c.remoteFIN = true
	c.finDSN = finalDSN
	c.addEventLocked("info", "received FIN at DSN %d (rcv_next=%d)", finalDSN, c.rcvNext)
	c.readCond.Broadcast()
	if c.rcvNext >= finalDSN {
		raw := wire.EncodeSimple(wire.TypeFINACK, c.connID, c.primaryID, 0, nil)
		return []outbound{{pathID: c.primaryID, raw: raw}}
	}
	return nil
}

func (c *Conn) planFINLocked() *outbound {
	if !c.localCloseWrite || c.finAcked {
		return nil
	}
	allAcked := c.sndNext == 0 || (c.sndUna >= c.sndNext && len(c.segs) == 0)
	if !allAcked {
		return nil
	}
	if !c.finSentAt.IsZero() && time.Since(c.finSentAt) < 50*time.Millisecond {
		return nil
	}
	c.finQueued = true
	c.finSentDSN = c.sndNext
	c.finSentAt = time.Now()
	raw := wire.EncodeFIN(c.connID, c.sndNext)
	pathID := c.primaryID
	if p := c.paths[c.primaryID]; p != nil && p.status != statusActive {
		for id, pp := range c.paths {
			if pp.status == statusActive {
				pathID = id
				break
			}
		}
	}
	return &outbound{pathID: pathID, raw: raw}
}

func (c *Conn) maybeRetransmitFINLocked() {
	// planFINLocked rate limits by finSentAt and is invoked by sendLoop after
	// workCond is broadcast here.
	if c.localCloseWrite && !c.finAcked {
		c.workCond.Broadcast()
	}
}
