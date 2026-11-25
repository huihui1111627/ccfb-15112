// Package simnet contains an in-process network emulator. Each path has its
// own bandwidth shaper, propagation delay, seeded loss/jitter and scheduled
// disconnect/reconnect behavior.
package simnet

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"clocktower/multipath/pkg/wire"
)

type MiddleboxMode string

const (
	MiddleboxNone         MiddleboxMode = ""
	MiddleboxMutateOption MiddleboxMode = "mutate-option"
	MiddleboxSplitMapping MiddleboxMode = "split-mapping"
)

// Profile describes one emulated path.
type Profile struct {
	PathID          uint16
	Name            string
	Priority        int
	RateBytesPerSec float64
	OneWayDelay     time.Duration
	Jitter          time.Duration
	LossRate        float64
	Seed            int64
	QueuePackets    int
	FlapEvery       time.Duration
	FlapDown        time.Duration
	Middlebox       MiddleboxMode
}

// Event reports path availability changes.
type Event struct {
	PathID uint16    `json:"path_id"`
	Up     bool      `json:"up"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// Link is one side of an emulated path.
type Link interface {
	Profile() Profile
	Send([]byte) error
	Recv() <-chan []byte
	Events() <-chan Event
	Up() bool
	Close() error
}

type linkState struct {
	up        atomic.Bool
	closeOnce sync.Once
	done      chan struct{}
}

type Endpoint struct {
	profile Profile
	state   *linkState
	egress  chan []byte
	recv    chan []byte
	events  chan Event
	rng     *rand.Rand
	seedDir int64
}

func (e *Endpoint) Profile() Profile     { return e.profile }
func (e *Endpoint) Recv() <-chan []byte  { return e.recv }
func (e *Endpoint) Events() <-chan Event { return e.events }
func (e *Endpoint) Up() bool             { return e.state.up.Load() }
func (e *Endpoint) Close() error         { e.state.closeOnce.Do(func() { close(e.state.done) }); return nil }

func (e *Endpoint) Send(pkt []byte) error {
	if !e.state.up.Load() {
		// A disconnected radio/NIC drops frames offered to it.
		return nil
	}
	select {
	case e.egress <- pkt:
		return nil
	case <-e.state.done:
		return errClosed
	default:
		// A full emulated qdisc applies tail drop rather than blocking the
		// protocol actor indefinitely.
		return nil
	}
}

var errClosed = &closedError{}

type closedError struct{}

func (*closedError) Error() string { return "simnet link closed" }

type Pair struct {
	Client *Endpoint
	Server *Endpoint
	state  *linkState
}

func NewPair(p Profile) *Pair {
	if p.QueuePackets <= 0 {
		p.QueuePackets = 2048
	}
	if p.RateBytesPerSec <= 0 {
		p.RateBytesPerSec = 125_000_000 // 1 Gbps
	}
	state := &linkState{done: make(chan struct{})}
	state.up.Store(true)

	mk := func(seedSalt int64) *Endpoint {
		seed := p.Seed + seedSalt
		return &Endpoint{
			profile: p,
			state:   state,
			egress:  make(chan []byte, p.QueuePackets),
			recv:    make(chan []byte, p.QueuePackets),
			events:  make(chan Event, 32),
			rng:     rand.New(rand.NewSource(seed)),
		}
	}
	c := mk(1)
	s := mk(1009)
	pair := &Pair{Client: c, Server: s, state: state}
	go runDirection(c, s, p, true, state)
	go runDirection(s, c, p, false, state)
	if p.FlapEvery > 0 && p.FlapDown > 0 {
		go runFlaps(p, state, c.events, s.events)
	}
	return pair
}

func runFlaps(p Profile, state *linkState, evs ...chan Event) {
	fire := func(up bool, reason string) {
		state.up.Store(up)
		ev := Event{PathID: p.PathID, Up: up, Reason: reason, At: time.Now()}
		for _, ch := range evs {
			select {
			case ch <- ev:
			default:
			}
		}
	}
	for {
		select {
		case <-state.done:
			return
		case <-time.After(p.FlapEvery):
		}
		fire(false, "simulated disconnect")
		select {
		case <-state.done:
			return
		case <-time.After(p.FlapDown):
		}
		fire(true, "link recovered")
	}
}

func runDirection(from *Endpoint, to *Endpoint, p Profile, clientToServer bool, state *linkState) {
	for {
		select {
		case <-state.done:
			return
		case pkt := <-from.egress:
			if !state.up.Load() {
				continue
			}
			chunks := [][]byte{pkt}
			if clientToServer {
				switch p.Middlebox {
				case MiddleboxMutateOption:
					if h, err := wire.DecodeHeader(pkt); err == nil && h.Type == wire.TypeData {
						chunks = [][]byte{wire.MutateMappingOption(pkt)}
					}
				case MiddleboxSplitMapping:
					if h, err := wire.DecodeHeader(pkt); err == nil && h.Type == wire.TypeData {
						chunks = wire.SplitMappingPacket(pkt)
					}
				}
			}
			for _, chunk := range chunks {
				shapeAndDeliver(chunk, from, to, p, state)
			}
		}
	}
}

func shapeAndDeliver(pkt []byte, from, to *Endpoint, p Profile, state *linkState) {
	// Serialization delay paces packets on this path. The egress loop waits
	// here, which is exactly the bandwidth shaper.
	if p.RateBytesPerSec > 0 {
		serial := time.Duration(float64(len(pkt)) * float64(time.Second) / p.RateBytesPerSec)
		select {
		case <-time.After(serial):
		case <-state.done:
			return
		}
	}
	if !state.up.Load() {
		return
	}
	if p.LossRate > 0 && from.rng.Float64() < p.LossRate {
		return
	}
	delay := p.OneWayDelay
	if p.Jitter > 0 {
		delay += time.Duration(from.rng.Int63n(int64(p.Jitter) + 1))
	}
	go func() {
		select {
		case <-time.After(delay):
			if !state.up.Load() {
				return
			}
			select {
			case to.recv <- pkt:
			case <-state.done:
			default:
				// Receiver inbox full: emulate tail drop.
			}
		case <-state.done:
		}
	}()
}

// Mbps converts megabits per second to bytes per second.
func Mbps(m float64) float64 { return m * 1_000_000 / 8 }
