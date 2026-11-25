package transport

import (
	"time"

	"clocktower/multipath/pkg/wire"
)

func joinAuthMAC(macKey []byte, connID uint64, b wire.JoinBody) []byte {
	var path [2]byte
	path[0] = byte(b.PathID)
	path[1] = byte(b.PathID >> 8)
	return wire.TruncatedMAC(macKey, wire.JoinMACBytes,
		[]byte("join-v1"), connIDBytes(connID), path[:], []byte{b.Priority}, b.Token)
}

func joinAckMAC(macKey []byte, connID uint64, pathID, subflowID uint16, status uint8) []byte {
	var b [5]byte
	b[0] = byte(pathID)
	b[1] = byte(pathID >> 8)
	b[2] = byte(subflowID)
	b[3] = byte(subflowID >> 8)
	b[4] = status
	return wire.TruncatedMAC(macKey, wire.JoinMACBytes, []byte("joinack-v1"), connIDBytes(connID), b[:])
}

func (c *Conn) openSubflowCountLocked() int {
	n := 0
	for _, p := range c.paths {
		if p.status != statusClosed {
			n++
		}
	}
	return n
}

func (c *Conn) reconcileAddresses() {
	c.mu.Lock()
	out := c.reconcileLocked()
	c.mu.Unlock()
	c.sendOutbound(out)
}

func (c *Conn) reconcileLocked() []outbound {
	if c.fallback {
		return nil
	}
	var out []outbound
	for _, a := range c.addresses {
		if a.PathID == c.primaryID {
			continue
		}
		if p := c.paths[a.PathID]; p != nil {
			continue
		}
		if c.openSubflowCountLocked() >= c.cfg.MaxSubflows {
			// Excess addresses remain recorded in c.addresses and are visible
			// in Snapshot. They are not dialed/JOINed until a slot closes.
			continue
		}
		o := c.startJoinLocked(a, false)
		if o != nil {
			out = append(out, *o)
		}
	}
	return out
}

func (c *Conn) retryJoinsLocked() []outbound {
	if c.keys == nil {
		return nil
	}
	var out []outbound
	now := time.Now()
	for _, p := range c.paths {
		if p.status != statusConnecting {
			continue
		}
		if !p.joinSentAt.IsZero() && now.Sub(p.joinSentAt) < 180*time.Millisecond {
			continue
		}
		body := wire.JoinBody{PathID: p.pathID, Priority: uint8(p.priority), Token: append([]byte(nil), c.keys.Token...)}
		body.MAC = joinAuthMAC(c.macKey, c.connID, body)
		raw := wire.EncodeJOIN(c.connID, 0, body)
		p.joinSentAt = now
		out = append(out, outbound{pathID: p.pathID, raw: raw})
	}
	return out
}

func (c *Conn) startJoinLocked(a Address, recovery bool) *outbound {
	p := newSubflow(a, c.nextSubID, false, c.cfg.InitialRTT)
	if recovery {
		p.status = statusConnecting
	}
	c.nextSubID++
	c.paths[a.PathID] = p
	c.cc.AddPath(a.PathID, p.srtt)

	body := wire.JoinBody{
		PathID:   a.PathID,
		Priority: uint8(a.Priority),
		Token:    append([]byte(nil), c.keys.Token...),
	}
	body.MAC = joinAuthMAC(c.macKey, c.connID, body)
	raw := wire.EncodeJOIN(c.connID, 0, body)
	p.joinSentAt = time.Now()
	c.addEventLocked("info", "sending JOIN on path %d (priority %d, recovery=%v)", a.PathID, a.Priority, recovery)
	return &outbound{pathID: a.PathID, raw: raw}
}

// CloseSubflow permanently closes one subflow slot and fills it with the
// highest-priority queued address. The primary path cannot be closed here.
func (c *Conn) CloseSubflow(pathID uint16) error {
	c.mu.Lock()
	p := c.paths[pathID]
	if p == nil {
		c.mu.Unlock()
		return ErrClosed
	}
	if p.isPrimary {
		c.mu.Unlock()
		return errPrimaryClose
	}
	p.status = statusClosed
	c.addEventLocked("warn", "subflow %d/path %d closed; reconciling queued addresses", p.id, pathID)
	closeRaw := wire.EncodeSimple(wire.TypeSubflowClose, c.connID, pathID, p.id, nil)
	c.workCond.Broadcast()
	c.mu.Unlock()
	// Notify over both the closing path and primary; loss on one must not make
	// the server retain the old slot and reject the priority replacement.
	c.sendOutbound([]outbound{{pathID: pathID, raw: closeRaw}, {pathID: c.primaryID, raw: closeRaw}})
	// Let the authenticated close be processed before JOINing the replacement,
	// avoiding a full-slot rejection caused by concurrent link goroutines.
	go func() {
		time.Sleep(120 * time.Millisecond)
		c.reconcileAddresses()
	}()
	return nil
}

var errPrimaryClose = &primaryCloseError{}

type primaryCloseError struct{}

func (*primaryCloseError) Error() string { return "primary path cannot be closed" }

func (c *Conn) handleJOIN(pkt []byte) []outbound {
	_, body, err := wire.DecodeJOIN(pkt)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keys == nil || c.closed {
		return nil
	}
	wantMAC := joinAuthMAC(c.macKey, c.connID, body)
	tokenOK := wire.EqualMAC(body.Token, c.keys.Token)
	macOK := wire.EqualMAC(body.MAC, wantMAC)
	if !tokenOK || !macOK {
		c.addEventLocked("warn", "JOIN auth failed on path %d", body.PathID)
		mac := joinAckMAC(c.macKey, c.connID, body.PathID, 0, wire.JoinStatusRejectAuth)
		return []outbound{{pathID: body.PathID, raw: wire.EncodeJOINACK(c.connID, body.PathID, 0, wire.JoinStatusRejectAuth, mac)}}
	}

	existing := c.paths[body.PathID]
	if existing == nil && c.openSubflowCountLocked() >= c.cfg.MaxSubflows {
		c.addEventLocked("warn", "JOIN path %d rejected: full", body.PathID)
		mac := joinAckMAC(c.macKey, c.connID, body.PathID, 0, wire.JoinStatusRejectFull)
		return []outbound{{pathID: body.PathID, raw: wire.EncodeJOINACK(c.connID, body.PathID, 0, wire.JoinStatusRejectFull, mac)}}
	}

	var id uint16
	if existing != nil {
		id = c.nextSubID
		c.nextSubID++
		existing.id = id
		existing.status = statusActive
		// SSN state and the reorder interval set intentionally survive a
		// temporary disconnect/rejoin: the path is authenticating the same
		// slot again, and old mappings remain valid for ACK/retries.
		id = existing.id
		c.addEventLocked("info", "path %d rejoined as subflow %d", body.PathID, id)
	} else {
		a := Address{PathID: body.PathID, Priority: int(body.Priority)}
		if l, ok := c.links[body.PathID]; ok {
			a.Link = l
		}
		p := newSubflow(a, c.nextSubID, false, c.cfg.InitialRTT)
		id = c.nextSubID
		c.nextSubID++
		p.status = statusActive
		c.paths[body.PathID] = p
		c.cc.AddPath(body.PathID, p.srtt)
		id = p.id
		c.addEventLocked("info", "path %d joined as subflow %d", body.PathID, id)
	}
	mac := joinAckMAC(c.macKey, c.connID, body.PathID, id, wire.JoinStatusOK)
	c.workCond.Broadcast()
	return []outbound{{pathID: body.PathID, raw: wire.EncodeJOINACK(c.connID, body.PathID, id, wire.JoinStatusOK, mac)}}
}

func (c *Conn) handleJOINACK(pkt []byte) []outbound {
	h, body, err := wire.DecodeJOINACK(pkt)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.paths[h.PathID]
	if p == nil {
		return nil
	}
	want := joinAckMAC(c.macKey, c.connID, body.PathID, body.SubflowID, body.Status)
	if !wire.EqualMAC(body.MAC, want) {
		c.addEventLocked("warn", "JOINACK MAC failed path %d", h.PathID)
		return nil
	}
	switch body.Status {
	case wire.JoinStatusOK:
		wasConnecting := p.status == statusConnecting
		p.id = body.SubflowID
		p.status = statusActive
		if !wasConnecting {
			p.rejoins++
		}
		c.addEventLocked("info", "path %d active with subflow id %d", p.pathID, p.id)
		c.workCond.Broadcast()
	case wire.JoinStatusRejectFull:
		p.status = statusClosed
		c.addEventLocked("warn", "path %d rejected because connection is full", p.pathID)
		out := c.reconcileLocked()
		c.workCond.Broadcast()
		return out
	default:
		p.status = statusClosed
	}
	return nil
}

// HandleLinkEvent updates path availability and immediately redirects old
// unacknowledged data when a simulated link goes down.
func (c *Conn) HandleLinkEvent(ev LinkEvent) []outbound {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.paths[ev.PathID]
	if p == nil {
		return nil
	}
	if !ev.Up {
		if p.status == statusClosed {
			return nil
		}
		p.status = statusDown
		p.downEvents++
		c.addEventLocked("warn", "path %d down: %s; redirecting outstanding data", ev.PathID, ev.Reason)
		c.cc.Timeout(ev.PathID, "link-down")
		// Remove in-flight accounting and mark every old transmission for
		// redirection on another active subflow.
		c.markPathDownLocked(ev.PathID)
		c.workCond.Broadcast()
		return nil
	}

	if p.status == statusClosed || c.fallback {
		return nil
	}
	if p.isPrimary {
		p.status = statusActive
		c.addEventLocked("info", "primary path %d recovered", ev.PathID)
		c.workCond.Broadcast()
		return nil
	}
	p.status = statusConnecting
	p.rejoins++
	body := wire.JoinBody{PathID: p.pathID, Priority: uint8(p.priority), Token: append([]byte(nil), c.keys.Token...)}
	body.MAC = joinAuthMAC(c.macKey, c.connID, body)
	raw := wire.EncodeJOIN(c.connID, 0, body)
	c.addEventLocked("info", "path %d recovered; rejoining with token and truncated MAC", ev.PathID)
	c.workCond.Broadcast()
	return []outbound{{pathID: p.pathID, raw: raw}}
}
