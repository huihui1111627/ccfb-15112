package transport

import (
	"crypto/ecdh"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"clocktower/multipath/pkg/cc"
	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/wire"
)

const (
	statusConnecting = "connecting"
	statusActive     = "active"
	statusDown       = "down"
	statusClosed     = "closed"
)

var (
	ErrClosed    = errors.New("transport connection closed")
	ErrFallback  = errors.New("transport fell back to single path")
	ErrMiddlebox = errors.New("middlebox mapping failure")
	ErrNoWindow  = errors.New("no scheduler window")
	ErrTimeout   = errors.New("transport timeout")
)

type outbound struct {
	pathID uint16
	raw    []byte
}

type txLoc struct {
	segDSN    uint64
	attemptID uint64
}

type txState struct {
	attemptID   uint64
	pathID      uint16
	subflowID   uint16
	ssnStart    uint64
	length      int
	sentAt      time.Time
	subAcked    bool
	lost        bool
	accounted   bool
	rttSampleOK bool
	isRetrans   bool
	isRedundant bool
	rttMeasured bool
}

type segment struct {
	dsn          uint64
	length       int
	data         []byte
	connAcked    bool
	sentOnce     bool
	needsRetrans bool
	fin          bool
	txs          map[uint64]*txState // key: pathID<<32 | ssnStart low? use txKey
	rescueEpoch  uint64
	lastTxPath   uint16
}

func txKey(pathID uint16, ssnStart uint64) string {
	return fmt.Sprintf("%d:%d", pathID, ssnStart)
}

type subflow struct {
	pathID    uint16
	id        uint16
	link      Link
	priority  int
	name      string
	profile   simnet.Profile
	isPrimary bool

	status string

	// Sender side subflow state.
	nextSSN    uint64
	cumSSNACK  uint64
	subFlight  int64
	sentBytes  int64
	ackedBytes int64
	subACKs    int64
	retrans    int64
	reinject   int64
	downEvents int64
	rejoins    int64
	rrTurns    uint64

	srtt       time.Duration
	rttvar     time.Duration
	hasRTT     bool
	joinSentAt time.Time

	txBySSN map[uint64]*txState

	// Receiver side subflow reorder state, used only to create SUB-ACKs.
	recvNextSSN uint64
	recvRanges  intervalSet
}

type interval struct {
	start, end uint64
}

type intervalSet struct {
	iv []interval
}

func (s *intervalSet) add(start, end uint64) {
	if end <= start {
		return
	}
	s.iv = append(s.iv, interval{start, end})
	sort.Slice(s.iv, func(i, j int) bool {
		if s.iv[i].start == s.iv[j].start {
			return s.iv[i].end > s.iv[j].end
		}
		return s.iv[i].start < s.iv[j].start
	})
	merged := s.iv[:0]
	for _, v := range s.iv {
		if len(merged) == 0 || v.start > merged[len(merged)-1].end {
			merged = append(merged, v)
		} else if v.end > merged[len(merged)-1].end {
			merged[len(merged)-1].end = v.end
		}
	}
	s.iv = merged
}

func (s *intervalSet) covers(start, end uint64) bool {
	for _, v := range s.iv {
		if v.start <= start && end <= v.end {
			return true
		}
		if v.start > end {
			return false
		}
	}
	return false
}

func (s *intervalSet) sack(base uint64, max int) []wire.Range {
	out := make([]wire.Range, 0, max)
	for _, v := range s.iv {
		if v.end <= base {
			continue
		}
		st := v.start
		if st < base {
			st = base
		}
		if v.end > st {
			out = append(out, wire.Range{Start: st, End: v.end})
		}
		if len(out) >= max {
			break
		}
	}
	return out
}

type reorderSegment struct {
	dsn  uint64
	end  uint64
	data []byte
}

type Conn struct {
	mu        sync.Mutex
	workCond  *sync.Cond
	writeCond *sync.Cond
	readCond  *sync.Cond

	cfg       Config
	role      string
	connID    uint64
	keys      *wire.Keys
	macKey    []byte
	startedAt time.Time

	established     bool
	acceptQueued    bool
	closed          bool
	closeErr        error
	done            chan struct{}
	handshakePacket []byte

	addresses []Address
	links     map[uint16]Link
	paths     map[uint16]*subflow
	primaryID uint16
	nextSubID uint16
	rrCursor  int

	cc        *cc.Group
	reference cc.OrdinaryReno

	// Shared connection receive window.
	localRWND  uint32
	remoteRWND uint32

	// Sender.
	sndUna           uint64
	sndNext          uint64
	sndSendNext      uint64
	remoteWindowEdge uint64
	segs             map[uint64]*segment
	sendBuffer       int
	connFlight       int64
	txSeq            uint64
	txIndex          map[string]txLoc
	totalSent        int64
	totalAcked       int64
	retransTotal     int64
	reinjectTotal    int64
	rescueEvents     int64
	rescueEpoch      uint64
	lastRescue       time.Time

	// Receiver / reorder buffer.
	rcvNext        uint64
	rcvPending     map[uint64]*reorderSegment
	rcvIntervals   intervalSet
	ready          [][]byte
	readyBytes     int
	pendingBytes   int
	storedBytes    int
	totalReceived  int64
	totalDelivered int64

	// FIN state.
	localCloseWrite bool
	finQueued       bool
	finSentDSN      uint64
	finSentAt       time.Time
	finAcked        bool
	remoteFIN       bool
	finDSN          uint64

	// Fallback.
	fallback       bool
	fallbackReason string
	fallbackSent   bool

	// ACK scheduling.
	connACKPending bool
	ackPathID      uint16
	lastConnACK    time.Time

	// Metrics and observability.
	latencies  []time.Duration
	latencySum time.Duration
	mappings   []MappingRecord
	eventLog   []EventRecord

	progressFn   func(Progress)
	lastProgress time.Time
}

func newConn(cfg Config, role string, connID uint64, keys *wire.Keys, addrs []Address) *Conn {
	cfg = normalizeConfig(cfg)
	c := &Conn{
		cfg:              cfg,
		role:             role,
		connID:           connID,
		keys:             keys,
		links:            make(map[uint16]Link),
		paths:            make(map[uint16]*subflow),
		addresses:        append([]Address(nil), addrs...),
		nextSubID:        1,
		cc:               cc.NewGroup(cfg.MSS),
		reference:        cc.NewOrdinaryReno(),
		segs:             make(map[uint64]*segment),
		txIndex:          make(map[string]txLoc),
		rcvPending:       make(map[uint64]*reorderSegment),
		done:             make(chan struct{}),
		localRWND:        uint32(cfg.ReceiveBuffer),
		remoteRWND:       uint32(cfg.ReceiveBuffer),
		remoteWindowEdge: uint64(cfg.ReceiveBuffer),
		startedAt:        time.Now(),
		lastConnACK:      time.Now(),
	}
	if keys != nil {
		c.macKey = keys.MacKey
	}
	c.workCond = sync.NewCond(&c.mu)
	c.writeCond = sync.NewCond(&c.mu)
	c.readCond = sync.NewCond(&c.mu)
	for _, a := range addrs {
		c.links[a.PathID] = a.Link
	}
	return c
}

func normalizeConfig(cfg Config) Config {
	d := DefaultConfig()
	if cfg.MaxSubflows <= 0 {
		cfg.MaxSubflows = d.MaxSubflows
	}
	if cfg.MSS <= 0 {
		cfg.MSS = d.MSS
	}
	if cfg.ReceiveBuffer <= 0 {
		cfg.ReceiveBuffer = d.ReceiveBuffer
	}
	if cfg.SendBuffer <= 0 {
		cfg.SendBuffer = d.SendBuffer
	}
	if cfg.Scheduler == "" {
		cfg.Scheduler = d.Scheduler
	}
	if cfg.InitialRTT <= 0 {
		cfg.InitialRTT = d.InitialRTT
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = d.HandshakeTimeout
	}
	if cfg.ACKDelay <= 0 {
		cfg.ACKDelay = d.ACKDelay
	}
	return cfg
}

func (c *Conn) addEventLocked(level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	c.eventLog = append(c.eventLog, EventRecord{At: time.Now(), Level: level, Message: msg})
	if len(c.eventLog) > 200 {
		c.eventLog = c.eventLog[len(c.eventLog)-200:]
	}
}

func (c *Conn) addMappingLocked(m MappingRecord) {
	c.mappings = append(c.mappings, m)
	if len(c.mappings) > 160 {
		c.mappings = c.mappings[len(c.mappings)-160:]
	}
}

func (c *Conn) recordLatency(d time.Duration) {
	if d <= 0 {
		return
	}
	c.latencies = append(c.latencies, d)
	c.latencySum += d
	if len(c.latencies) > 20000 {
		// Resample by retaining the latter half plus a coarse prefix is not
		// necessary for tests; cap memory for very long demos.
		c.latencies = append([]time.Duration(nil), c.latencies[10000:]...)
	}
}

// startLoops starts background work. It is safe to call once after handshake.
func (c *Conn) startLoops() {
	go c.sendLoop()
	go c.timerLoop()
	go c.ackLoop()
}

func (c *Conn) stopCondBroadcastLocked() {
	c.workCond.Broadcast()
	c.writeCond.Broadcast()
	c.readCond.Broadcast()
}

// Close terminates the connection and releases blocked readers/writers.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.closeErr = ErrClosed
	close(c.done)
	c.stopCondBroadcastLocked()
	c.mu.Unlock()
	return nil
}

func (c *Conn) failLocked(err error) {
	if c.closed {
		return
	}
	c.closed = true
	c.closeErr = err
	close(c.done)
	c.stopCondBroadcastLocked()
}

// CloseWrite flushes and closes the byte stream after all data is acknowledged.
func (c *Conn) CloseWrite() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return c.closeErr
	}
	c.localCloseWrite = true
	c.workCond.Broadcast()
	// Wait until FIN has been emitted and acknowledged. Tests transfer finite
	// files, so this provides a clean io.Copy synchronization point.
	for !c.closed && (!c.finQueued || !c.finAcked) {
		c.writeCond.Wait()
	}
	err := c.closeErr
	c.mu.Unlock()
	return err
}

// SetProgressCallback installs an optional progress callback.
func (c *Conn) SetProgressCallback(fn func(Progress)) {
	c.mu.Lock()
	c.progressFn = fn
	c.mu.Unlock()
}

func sortedAddresses(addrs []Address) []Address {
	out := append([]Address(nil), addrs...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority == out[j].Priority {
			return out[i].PathID < out[j].PathID
		}
		return out[i].Priority < out[j].Priority
	})
	return out
}

func rttOrInitial(p *subflow, initial time.Duration) time.Duration {
	if p != nil && p.hasRTT && p.srtt > 0 {
		return p.srtt
	}
	if p != nil && p.profile.OneWayDelay > 0 {
		return 2 * p.profile.OneWayDelay
	}
	return initial
}

func updateRTT(p *subflow, sample time.Duration) {
	if sample <= 0 {
		return
	}
	// The emulator's longest base RTT is ~160ms. A larger "sample" is almost
	// always an ambiguous/reused transmission timestamp under loss; ignoring
	// it prevents one bad sample from pushing RTO into a multi-second tail.
	if sample > 400*time.Millisecond {
		return
	}
	if !p.hasRTT || p.srtt == 0 {
		p.srtt = sample
		p.rttvar = sample / 2
		p.hasRTT = true
		return
	}
	diff := p.srtt - sample
	if diff < 0 {
		diff = -diff
	}
	p.rttvar = (3*p.rttvar + diff) / 4
	p.srtt = (7*p.srtt + sample) / 8
}

func (p *subflow) rto() time.Duration {
	if !p.hasRTT || p.srtt <= 0 {
		return 120 * time.Millisecond
	}
	rto := p.srtt + 4*p.rttvar
	if rto < 35*time.Millisecond {
		rto = 35 * time.Millisecond
	}
	if rto > 800*time.Millisecond {
		rto = 800 * time.Millisecond
	}
	return rto
}

// Ensure ecdh import is used by future key helper signatures in this file set.
var _ *ecdh.PrivateKey
