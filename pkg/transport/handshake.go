package transport

import (
	"context"
	"crypto/ecdh"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"clocktower/multipath/pkg/wire"
)

func connIDBytes(id uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], id)
	return b[:]
}

func confirmMAC(macKey []byte, label string, connID uint64, extra ...[]byte) []byte {
	parts := [][]byte{[]byte(label), connIDBytes(connID)}
	parts = append(parts, extra...)
	return wire.TruncatedMAC(macKey, wire.JoinMACBytes, parts...)
}

func newSubflow(a Address, id uint16, primary bool, initialRTT time.Duration) *subflow {
	p := &subflow{
		pathID:    a.PathID,
		id:        id,
		link:      a.Link,
		priority:  a.Priority,
		isPrimary: primary,
		status:    statusConnecting,
		txBySSN:   make(map[uint64]*txState),
	}
	if a.Link != nil {
		pr := a.Link.Profile()
		p.profile = pr
		p.name = pr.Name
		if pr.Priority != 0 && a.Priority == 0 {
			p.priority = pr.Priority
		}
		if pr.OneWayDelay > 0 {
			p.srtt = 2 * pr.OneWayDelay
			p.rttvar = pr.OneWayDelay / 2
			p.hasRTT = true
		} else if initialRTT > 0 {
			p.srtt = initialRTT
			p.rttvar = initialRTT / 4
			p.hasRTT = true
		}
	}
	return p
}

// Dial establishes the primary path, performs ephemeral X25519 key exchange,
// then joins additional addresses within the configured subflow limit.
func Dial(ctx context.Context, cfg Config, addrs []Address) (*Conn, error) {
	if len(addrs) == 0 {
		return nil, errors.New("transport: at least one address is required")
	}
	cfg = normalizeConfig(cfg)
	ordered := sortedAddresses(addrs)
	primaryAddr := ordered[0]

	priv, err := wire.ClientKey()
	if err != nil {
		return nil, err
	}
	connID, err := wire.RandomUint64()
	if err != nil {
		return nil, err
	}
	nonceC, err := wire.RandomBytes(wire.NonceBytes)
	if err != nil {
		return nil, err
	}
	clientPub := priv.PublicKey().Bytes()
	syn := wire.EncodeSYN(connID, nonceC, clientPub)

	deadline := time.Now().Add(cfg.HandshakeTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	var keys *wire.Keys
	var serverNonce, serverPub []byte

	for time.Now().Before(deadline) {
		if err := primaryAddr.Link.Send(syn); err != nil {
			return nil, err
		}
		wait := time.NewTimer(200 * time.Millisecond)
		select {
		case pkt := <-primaryAddr.Link.Recv():
			wait.Stop()
			h, err := wire.DecodeHeader(pkt)
			if err != nil || h.Type != wire.TypeSYNACK || h.ConnID != connID {
				continue
			}
			nc, pub, mac, err := wire.DecodeSYNACK(pkt)
			if err != nil {
				continue
			}
			_ = nc // SYNACK nonce is server nonce by wire position.
			serverNonce, serverPub, err = parseSYNACKNonce(pkt)
			if err != nil {
				continue
			}
			peerPub, err := ecdh.X25519().NewPublicKey(pub)
			if err != nil {
				continue
			}
			shared, err := priv.ECDH(peerPub)
			if err != nil {
				continue
			}
			keys, err = wire.DeriveKeys(shared, nonceC, serverNonce, connIDBytes(connID))
			if err != nil {
				return nil, err
			}
			want := confirmMAC(keys.MacKey, "synack-v1", connID, nonceC, serverNonce, serverPub)
			if !wire.EqualMAC(mac, want) {
				return nil, wire.ErrAuth
			}
			hackMAC := confirmMAC(keys.MacKey, "hack-v1", connID)
			hack := wire.EncodeHandshakeACK(connID, hackMAC)
			_ = primaryAddr.Link.Send(hack)
			goto established
		case <-wait.C:
			wait.Stop()
		case <-ctx.Done():
			wait.Stop()
			return nil, ctx.Err()
		}
	}
	return nil, ErrTimeout

established:
	c := newConn(cfg, "client", connID, keys, ordered)
	c.primaryID = primaryAddr.PathID
	p := newSubflow(primaryAddr, c.nextSubID, true, cfg.InitialRTT)
	c.nextSubID++
	p.status = statusActive
	c.paths[p.pathID] = p
	c.cc.AddPath(p.pathID, p.srtt)
	c.established = true
	c.addEventLocked("info", "primary path %d established; key exchange complete", p.pathID)
	if err := waitClientHandshakeConfirm(ctx, c, primaryAddr.Link); err != nil {
		return nil, err
	}
	c.startLoops()
	for _, a := range c.addresses {
		go c.clientLinkLoop(a)
	}
	c.reconcileAddresses()
	return c, nil
}

// DecodeSYNACK returns nonce first in the API; this helper makes the semantic
// explicit after the initial decode already validated lengths.
func parseSYNACKNonce(pkt []byte) (nonce, pub []byte, err error) {
	nonce, pub, _, err = wire.DecodeSYNACK(pkt)
	return nonce, pub, err
}

func waitClientHandshakeConfirm(ctx context.Context, c *Conn, link Link) error {
	mac := confirmMAC(c.macKey, "hack-v1", c.connID)
	hack := wire.EncodeHandshakeACK(c.connID, mac)
	_ = link.Send(hack)

	ticker := time.NewTicker(80 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return ErrClosed
		case <-deadline.C:
			return ErrTimeout
		case <-ticker.C:
			_ = link.Send(hack)
		case pkt := <-link.Recv():
			h, err := wire.DecodeHeader(pkt)
			if err == nil && h.ConnID == c.connID && h.Type == wire.TypeConnectionACK {
				return nil
			}
		}
	}
}

func (c *Conn) clientLinkLoop(a Address) {
	for {
		select {
		case <-c.done:
			return
		case pkt, ok := <-a.Link.Recv():
			if !ok {
				return
			}
			out := c.HandlePacket(a.PathID, pkt)
			c.sendOutbound(out)
		case ev, ok := <-a.Link.Events():
			if !ok {
				return
			}
			out := c.HandleLinkEvent(ev)
			c.sendOutbound(out)
		}
	}
}

func (c *Conn) sendOutbound(out []outbound) {
	for _, o := range out {
		c.mu.Lock()
		link := c.links[o.pathID]
		c.mu.Unlock()
		if link != nil {
			_ = link.Send(o.raw)
		}
	}
}

// Server accepts clocktower multipath connections.
type Server struct {
	cfg      Config
	addrs    []Address
	mu       sync.Mutex
	conns    map[uint64]*Conn
	acceptCh chan *Conn
	done     chan struct{}
}

func Listen(cfg Config, addrs []Address) *Server {
	cfg = normalizeConfig(cfg)
	s := &Server{
		cfg:      cfg,
		addrs:    append([]Address(nil), addrs...),
		conns:    make(map[uint64]*Conn),
		acceptCh: make(chan *Conn, 16),
		done:     make(chan struct{}),
	}
	for _, a := range addrs {
		go s.linkLoop(a)
	}
	return s
}

func (s *Server) Close() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
}

func (s *Server) Accept(ctx context.Context) (*Conn, error) {
	select {
	case c := <-s.acceptCh:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrClosed
	}
}

func (s *Server) linkLoop(a Address) {
	for {
		select {
		case <-s.done:
			return
		case pkt, ok := <-a.Link.Recv():
			if !ok {
				return
			}
			for _, o := range s.handlePacket(a, pkt) {
				if l := s.linkByID(o.pathID); l != nil {
					_ = l.Send(o.raw)
				} else {
					_ = a.Link.Send(o.raw)
				}
			}
		case ev, ok := <-a.Link.Events():
			if !ok {
				return
			}
			for _, c := range s.allConns() {
				for _, o := range c.HandleLinkEvent(ev) {
					if l := s.linkByID(o.pathID); l != nil {
						_ = l.Send(o.raw)
					}
				}
			}
		}
	}
}

func (s *Server) allConns() []*Conn {
	s.mu.Lock()
	out := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c)
	}
	s.mu.Unlock()
	return out
}

func (s *Server) linkByID(id uint16) Link {
	for _, a := range s.addrs {
		if a.PathID == id {
			return a.Link
		}
	}
	return nil
}

func (s *Server) handlePacket(a Address, pkt []byte) []outbound {
	h, err := wire.DecodeHeader(pkt)
	if err != nil {
		return nil
	}

	s.mu.Lock()
	c := s.conns[h.ConnID]
	s.mu.Unlock()

	if h.Type == wire.TypeSYN {
		return s.handleSYN(a, pkt)
	}
	if c == nil {
		return nil
	}
	var out []outbound
	if h.Type == wire.TypeHandshakeACK {
		out = c.handleHandshakeACK(pkt)
	} else {
		out = c.HandlePacket(a.PathID, pkt)
	}
	s.offerAccepted(c)
	return out
}

func (s *Server) offerAccepted(c *Conn) {
	c.mu.Lock()
	if !c.established || c.acceptQueued {
		c.mu.Unlock()
		return
	}
	c.acceptQueued = true
	c.mu.Unlock()
	select {
	case s.acceptCh <- c:
	default:
		// Accept channel has a small buffer; a queued entry is sufficient.
	}
}

func (s *Server) handleSYN(a Address, pkt []byte) []outbound {
	h, err := wire.DecodeHeader(pkt)
	if err != nil {
		return nil
	}
	s.mu.Lock()
	existing := s.conns[h.ConnID]
	s.mu.Unlock()
	if existing != nil {
		// SYN retransmissions must not rotate ECDH keys underneath the client.
		existing.mu.Lock()
		synack := append([]byte(nil), existing.handshakePacket...)
		existing.mu.Unlock()
		if len(synack) > 0 {
			return []outbound{{pathID: a.PathID, raw: synack}}
		}
	}
	nonceC, pubBytes, err := wire.DecodeSYN(pkt)
	if err != nil {
		return nil
	}
	pub, err := ecdh.X25519().NewPublicKey(pubBytes)
	if err != nil {
		return nil
	}
	priv, err := wire.ClientKey()
	if err != nil {
		return nil
	}
	nonceS, err := wire.RandomBytes(wire.NonceBytes)
	if err != nil {
		return nil
	}
	shared, err := priv.ECDH(pub)
	if err != nil {
		return nil
	}
	keys, err := wire.DeriveKeys(shared, nonceC, nonceS, connIDBytes(h.ConnID))
	if err != nil {
		return nil
	}
	c := newConn(s.cfg, "server", h.ConnID, keys, s.addrs)
	c.primaryID = a.PathID
	p := newSubflow(a, c.nextSubID, true, s.cfg.InitialRTT)
	c.nextSubID++
	c.paths[p.pathID] = p
	c.cc.AddPath(p.pathID, p.srtt)

	s.mu.Lock()
	s.conns[h.ConnID] = c
	s.mu.Unlock()

	serverPub := priv.PublicKey().Bytes()
	mac := confirmMAC(keys.MacKey, "synack-v1", h.ConnID, nonceC, nonceS, serverPub)
	synack := wire.EncodeSYNACK(h.ConnID, nonceS, serverPub, mac)
	c.handshakePacket = append([]byte(nil), synack...)
	return []outbound{{pathID: a.PathID, raw: synack}}
}

func (c *Conn) handleHandshakeACK(pkt []byte) []outbound {
	h, mac, err := wire.DecodeMACOnly(pkt, wire.TypeHandshakeACK, wire.JoinMACBytes)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.established || c.keys == nil {
		if c.established {
			c.establishLocked()
		}
		return nil
	}
	// The server retained client nonce implicitly only through key derivation;
	// key possession is proven by a valid HACK over connection id and label.
	want := confirmMAC(c.macKey, "hack-v1", h.ConnID)
	if !wire.EqualMAC(mac, want) {
		c.addEventLocked("warn", "bad handshake confirmation MAC")
		return nil
	}
	c.establishLocked()
	return nil
}

func (c *Conn) establishedNow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.established
}

func (c *Conn) establishLocked() {
	if c.established {
		return
	}
	c.established = true
	if p := c.paths[c.primaryID]; p != nil {
		p.status = statusActive
	}
	c.addEventLocked("info", "handshake confirmed")
	c.startLoops()
}
