package transport

import (
	"time"

	"clocktower/multipath/pkg/cc"
	"clocktower/multipath/pkg/simnet"
)

type SchedulerKind string

const (
	SchedulerMinRTT     SchedulerKind = "min-rtt"
	SchedulerRoundRobin SchedulerKind = "round-robin"
	SchedulerSingle     SchedulerKind = "single"
)

type Config struct {
	MaxSubflows   int
	MSS           int
	ReceiveBuffer int
	SendBuffer    int
	Scheduler     SchedulerKind
	// InitialRTT is used only before the first measured sample. Real RTT is
	// subsequently derived from subflow ACKs.
	InitialRTT       time.Duration
	HandshakeTimeout time.Duration
	ACKDelay         time.Duration
	// ProgressInterval controls monitor callbacks; zero disables callbacks.
	ProgressInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxSubflows:      4,
		MSS:              cc.DefaultMSS,
		ReceiveBuffer:    48 << 20,
		SendBuffer:       96 << 20,
		Scheduler:        SchedulerMinRTT,
		InitialRTT:       80 * time.Millisecond,
		HandshakeTimeout: 10 * time.Second,
		ACKDelay:         2 * time.Millisecond,
	}
}

// Link is the network service required by transport. simnet.Link is the
// normal implementation.
type Link = simnet.Link
type LinkEvent = simnet.Event

type Address struct {
	PathID   uint16
	Priority int
	Link     Link
}

type PathSnapshot struct {
	PathID          uint16        `json:"path_id"`
	SubflowID       uint16        `json:"subflow_id"`
	Status          string        `json:"status"`
	Priority        int           `json:"priority"`
	Name            string        `json:"name"`
	RateBps         float64       `json:"rate_bytes_sec"`
	BaseDelay       time.Duration `json:"base_delay_ns"`
	LossRate        float64       `json:"loss_rate"`
	RTT             time.Duration `json:"rtt_ns"`
	CWND            float64       `json:"cwnd_mss"`
	CWNDBytes       int           `json:"cwnd_bytes"`
	Ssthresh        float64       `json:"ssthresh_mss"`
	SubflightBytes  int64         `json:"subflight_bytes"`
	SSNNext         uint64        `json:"ssn_next"`
	SSNCumACK       uint64        `json:"ssn_cum_ack"`
	SentBytes       int64         `json:"sent_bytes"`
	AckedBytes      int64         `json:"acked_bytes"`
	SubACKs         int64         `json:"subacks"`
	Retransmissions int64         `json:"retransmissions"`
	Reinjections    int64         `json:"reinjections"`
	DownEvents      int64         `json:"down_events"`
	Rejoins         int64         `json:"rejoins"`
	RoundRobinTurns uint64        `json:"round_robin_turns"`
}

type MappingRecord struct {
	At        time.Time `json:"at"`
	PathID    uint16    `json:"path_id"`
	SubflowID uint16    `json:"subflow_id"`
	DSNStart  uint64    `json:"dsn_start"`
	DSNEnd    uint64    `json:"dsn_end"`
	SSNStart  uint64    `json:"ssn_start"`
	SSNEnd    uint64    `json:"ssn_end"`
	Length    int       `json:"length"`
	Flags     uint8     `json:"flags"`
	Reason    string    `json:"reason"`
}

type ReorderSegment struct {
	DSNStart uint64 `json:"dsn_start"`
	DSNEnd   uint64 `json:"dsn_end"`
	Length   int    `json:"length"`
}

type ReorderSnapshot struct {
	NextDSN      uint64           `json:"next_dsn"`
	FinDSN       uint64           `json:"fin_dsn,omitempty"`
	HasFIN       bool             `json:"has_fin"`
	Buffered     int              `json:"buffered_bytes"`
	Capacity     int              `json:"capacity_bytes"`
	ReadyBytes   int              `json:"ready_bytes"`
	PendingBytes int              `json:"pending_bytes"`
	SegmentCount int              `json:"segment_count"`
	Segments     []ReorderSegment `json:"segments"`
	Gaps         []wireRangeJSON  `json:"gaps"`
}

type wireRangeJSON struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type EventRecord struct {
	At      time.Time `json:"at"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

type Snapshot struct {
	Connected       bool              `json:"connected"`
	Role            string            `json:"role"`
	Scheduler       SchedulerKind     `json:"scheduler"`
	Mode            string            `json:"mode"`
	ConnID          string            `json:"conn_id"`
	MaxSubflows     int               `json:"max_subflows"`
	ActiveSubflows  int               `json:"active_subflows"`
	QueuedAddrs     int               `json:"queued_count"`
	DSNUna          uint64            `json:"dsn_una"`
	DSNNext         uint64            `json:"dsn_next"`
	ConnFlight      int64             `json:"conn_flight_bytes"`
	RemoteRWND      uint32            `json:"remote_rwnd"`
	LocalRWND       uint32            `json:"local_rwnd"`
	SendBuffer      int               `json:"send_buffer_bytes"`
	TotalSent       int64             `json:"total_sent_bytes"`
	TotalAcked      int64             `json:"total_acked_bytes"`
	TotalReceived   int64             `json:"total_received_bytes"`
	TotalDelivered  int64             `json:"total_delivered_bytes"`
	Retransmissions int64             `json:"retransmissions"`
	Reinjections    int64             `json:"reinjections"`
	RescueEvents    int64             `json:"rescue_events"`
	FallbackReason  string            `json:"fallback_reason,omitempty"`
	Alpha           float64           `json:"alpha"`
	ReferenceCWND   float64           `json:"reference_cwnd_mss"`
	LatencyAvgMS    float64           `json:"latency_avg_ms"`
	LatencyP95MS    float64           `json:"latency_p95_ms"`
	StartedAt       time.Time         `json:"started_at"`
	Paths           []PathSnapshot    `json:"paths"`
	QueuedAddresses []AddressSnapshot `json:"queued_addresses"`
	Mappings        []MappingRecord   `json:"mappings"`
	Reorder         ReorderSnapshot   `json:"reorder"`
	Events          []EventRecord     `json:"events"`
	FormulaSteps    []cc.Step         `json:"formula_steps"`
}

type AddressSnapshot struct {
	PathID   uint16 `json:"path_id"`
	Priority int    `json:"priority"`
	Name     string `json:"name"`
}

type Progress struct {
	BytesTotal     int64
	BytesComplete  int64
	ThroughputBps  float64
	ActiveSubflows int
	Mode           string
}
