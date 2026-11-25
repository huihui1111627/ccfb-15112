package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"clocktower/multipath/internal/testkit"
	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

//go:embed static/*
var staticFS embed.FS

type StrategyResult struct {
	Strategy        transport.SchedulerKind `json:"strategy"`
	Bytes           int64                   `json:"bytes"`
	DurationMS      int64                   `json:"duration_ms"`
	ThroughputMBps  float64                 `json:"throughput_mbps"`
	LatencyAvgMS    float64                 `json:"latency_avg_ms"`
	LatencyP95MS    float64                 `json:"latency_p95_ms"`
	Retransmissions int64                   `json:"retransmissions"`
	RescueEvents    int64                   `json:"rescue_events"`
	Fallback        bool                    `json:"fallback"`
	At              time.Time               `json:"at"`
	Error           string                  `json:"error,omitempty"`
}

type State struct {
	Running         bool                    `json:"running"`
	Loop            bool                    `json:"loop"`
	ActiveScheduler transport.SchedulerKind `json:"active_scheduler"`
	Snapshot        *transport.Snapshot     `json:"snapshot,omitempty"`
	LastSnapshot    *transport.Snapshot     `json:"last_snapshot,omitempty"`
	Comparison      []StrategyResult        `json:"comparison"`
	History         []StrategyResult        `json:"history"`
	UpdatedAt       time.Time               `json:"updated_at"`
}

type Manager struct {
	mu         sync.Mutex
	active     *transport.Conn
	last       *transport.Snapshot
	comparison []StrategyResult
	history    []StrategyResult
	running    bool
	loop       bool
	scheduler  transport.SchedulerKind
	cancelLoop context.CancelFunc
}

func NewManager() *Manager {
	return &Manager{comparison: []StrategyResult{}, history: []StrategyResult{}}
}

func demoProfiles() []simnet.Profile {
	return []simnet.Profile{
		{PathID: 1, Name: "mobile-5ms", Priority: 1, RateBytesPerSec: simnet.Mbps(650), OneWayDelay: 5 * time.Millisecond, LossRate: 0.006, Seed: 4242},
		{PathID: 2, Name: "radio-50ms-flap", Priority: 2, RateBytesPerSec: simnet.Mbps(220), OneWayDelay: 50 * time.Millisecond, LossRate: 0.025, Seed: 9999, FlapEvery: 1200 * time.Millisecond, FlapDown: 220 * time.Millisecond},
		{PathID: 3, Name: "backup-15ms", Priority: 3, RateBytesPerSec: simnet.Mbps(380), OneWayDelay: 15 * time.Millisecond, LossRate: 0.01, Seed: 777},
	}
}

func randomReader(seed int64, n int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), n)
}

func (m *Manager) runTransfer(ctx context.Context, scheduler transport.SchedulerKind, sizeMB int, profiles []simnet.Profile) StrategyResult {
	size := int64(sizeMB) << 20
	hk, rep, err := testkit.Run(ctx, testkit.Options{
		Name: "dashboard", Size: size, Profiles: profiles, Scheduler: scheduler,
		MaxSubflows: len(profiles), Timeout: 2 * time.Minute,
		OnReady: func(h *testkit.Harness) {
			m.mu.Lock()
			m.active = h.ClientConn
			m.running = true
			m.scheduler = scheduler
			m.mu.Unlock()
		},
	}, randomReader(time.Now().UnixNano(), size), io.Discard)
	res := StrategyResult{Strategy: scheduler, Bytes: size, At: time.Now()}
	if err != nil {
		res.Error = err.Error()
	} else {
		res.DurationMS = rep.Duration.Milliseconds()
		res.ThroughputMBps = rep.Throughput / 1e6
		if rep.Client != nil {
			res.LatencyAvgMS = rep.Client.LatencyAvgMS
			res.LatencyP95MS = rep.Client.LatencyP95MS
			res.Retransmissions = rep.Client.Retransmissions
			res.RescueEvents = rep.Client.RescueEvents
			res.Fallback = rep.Client.Mode != "multipath"
			m.mu.Lock()
			m.last = rep.Client
			m.mu.Unlock()
		}
	}
	if hk != nil {
		hk.Close()
	}
	m.mu.Lock()
	m.active = nil
	m.running = false
	m.history = append(m.history, res)
	if len(m.history) > 30 {
		m.history = m.history[len(m.history)-30:]
	}
	m.mu.Unlock()
	return res
}

// RunComparison executes both schedulers on identical link profiles.
func (m *Manager) RunComparison(sizeMB int) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
		defer cancel()
		profiles := demoProfiles()
		r1 := m.runTransfer(ctx, transport.SchedulerMinRTT, sizeMB, profiles)
		r2 := m.runTransfer(ctx, transport.SchedulerRoundRobin, sizeMB, profiles)
		m.mu.Lock()
		m.comparison = []StrategyResult{r1, r2}
		m.mu.Unlock()
	}()
}

func (m *Manager) StartLoop(sizeMB int) {
	m.mu.Lock()
	if m.loop {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.loop = true
	m.cancelLoop = cancel
	m.mu.Unlock()
	go func() {
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			sched := transport.SchedulerMinRTT
			if i%2 == 1 {
				sched = transport.SchedulerRoundRobin
			}
			res := m.runTransfer(ctx, sched, sizeMB, demoProfiles())
			if res.Error == "" {
				m.mu.Lock()
				m.comparison = upsertStrategy(m.comparison, res)
				m.mu.Unlock()
			}
			i++
			select {
			case <-ctx.Done():
				return
			case <-time.After(2500 * time.Millisecond):
			}
		}
	}()
}

func upsertStrategy(rs []StrategyResult, r StrategyResult) []StrategyResult {
	for i := range rs {
		if rs[i].Strategy == r.Strategy {
			rs[i] = r
			return rs
		}
	}
	return append(rs, r)
}

func (m *Manager) StopLoop() {
	m.mu.Lock()
	m.loop = false
	cancel := m.cancelLoop
	m.cancelLoop = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (m *Manager) currentState() State {
	m.mu.Lock()
	active := m.active
	last := m.last
	running := m.running
	loop := m.loop
	sched := m.scheduler
	comp := append([]StrategyResult(nil), m.comparison...)
	hist := append([]StrategyResult(nil), m.history...)
	m.mu.Unlock()
	var snap *transport.Snapshot
	if active != nil {
		snap = active.Snapshot()
	}
	return State{
		Running: running, Loop: loop, ActiveScheduler: sched,
		Snapshot: snap, LastSnapshot: last, Comparison: comp, History: hist,
		UpdatedAt: time.Now(),
	}
}

type Server struct {
	mgr *Manager
	mux *http.ServeMux
}

func NewServer(mgr *Manager) *Server {
	s := &Server{mgr: mgr, mux: http.NewServeMux()}
	s.mux.Handle("/static/", http.FileServer(http.FS(staticFS)))
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		b, err := staticFS.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(b)
	})
	s.mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mgr.currentState())
	})
	s.mux.HandleFunc("/api/compare", func(w http.ResponseWriter, r *http.Request) {
		size := 32
		if v := r.URL.Query().Get("sizeMB"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				size = n
			}
		}
		mgr.RunComparison(size)
		_, _ = w.Write([]byte(`{"started":true}`))
	})
	s.mux.HandleFunc("/api/loop", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stop") == "1" {
			mgr.StopLoop()
		} else {
			mgr.StartLoop(32)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }
