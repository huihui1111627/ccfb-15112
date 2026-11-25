// Package testkit constructs in-process client/server multipath transfers for
// tests and the standalone matrix benchmark.
package testkit

import (
	"context"
	"fmt"
	"io"
	"time"

	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

type Options struct {
	Name        string
	Size        int64
	Profiles    []simnet.Profile
	Scheduler   transport.SchedulerKind
	MaxSubflows int
	MSS         int
	RcvBuffer   int
	SndBuffer   int
	Timeout     time.Duration
	OnReady     func(*Harness)
}

type Report struct {
	Name       string
	Bytes      int64
	Duration   time.Duration
	Throughput float64 // bytes/second
	Client     *transport.Snapshot
	Server     *transport.Snapshot
	Strategy   transport.SchedulerKind
	Fallback   bool
}

type Harness struct {
	Server     *transport.Server
	ServerConn *transport.Conn
	ClientConn *transport.Conn
	Pairs      []*simnet.Pair
	cancel     context.CancelFunc
}

func (h *Harness) Close() {
	if h.cancel != nil {
		h.cancel()
	}
	if h.ClientConn != nil {
		_ = h.ClientConn.Close()
	}
	if h.ServerConn != nil {
		_ = h.ServerConn.Close()
	}
	if h.Server != nil {
		h.Server.Close()
	}
	for _, p := range h.Pairs {
		_ = p.Client.Close()
		_ = p.Server.Close()
	}
}

// Run executes one finite transfer. The caller owns src/dst and can wrap them
// with hashers. Returned harness remains available for snapshots until Close.
func Run(ctx context.Context, opt Options, src io.Reader, dst io.Writer) (*Harness, Report, error) {
	if len(opt.Profiles) == 0 {
		return nil, Report{}, fmt.Errorf("testkit: no link profiles")
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, opt.Timeout)

	pairs := make([]*simnet.Pair, 0, len(opt.Profiles))
	clientAddrs := make([]transport.Address, 0, len(opt.Profiles))
	serverAddrs := make([]transport.Address, 0, len(opt.Profiles))
	for _, pr := range opt.Profiles {
		p := simnet.NewPair(pr)
		pairs = append(pairs, p)
		clientAddrs = append(clientAddrs, transport.Address{
			PathID: pr.PathID, Priority: pr.Priority, Link: p.Client,
		})
		serverAddrs = append(serverAddrs, transport.Address{
			PathID: pr.PathID, Priority: pr.Priority, Link: p.Server,
		})
	}

	cfg := transport.DefaultConfig()
	cfg.Scheduler = opt.Scheduler
	if opt.MaxSubflows > 0 {
		cfg.MaxSubflows = opt.MaxSubflows
	}
	if opt.MSS > 0 {
		cfg.MSS = opt.MSS
	}
	if opt.RcvBuffer > 0 {
		cfg.ReceiveBuffer = opt.RcvBuffer
	}
	if opt.SndBuffer > 0 {
		cfg.SendBuffer = opt.SndBuffer
	}

	srv := transport.Listen(cfg, serverAddrs)
	h := &Harness{Server: srv, Pairs: pairs, cancel: cancel}

	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	client, err := transport.Dial(dialCtx, cfg, clientAddrs)
	dialCancel()
	if err != nil {
		h.Close()
		return nil, Report{}, fmt.Errorf("dial: %w", err)
	}
	h.ClientConn = client
	go func() {
		<-ctx.Done()
		h.Close()
	}()

	acceptCtx, acceptCancel := context.WithTimeout(ctx, 10*time.Second)
	sconn, err := srv.Accept(acceptCtx)
	acceptCancel()
	if err != nil {
		h.Close()
		return nil, Report{}, fmt.Errorf("accept: %w", err)
	}
	h.ServerConn = sconn
	if opt.OnReady != nil {
		opt.OnReady(h)
	}

	start := time.Now()
	sendErr := make(chan error, 1)
	go func() {
		_, err := io.Copy(client, src)
		if err == nil {
			err = client.CloseWrite()
		}
		sendErr <- err
	}()

	recvN, copyErr := io.Copy(dst, sconn)
	if copyErr != nil {
		h.Close()
		return h, Report{}, fmt.Errorf("receive: %w", copyErr)
	}
	select {
	case err := <-sendErr:
		if err != nil {
			h.Close()
			return h, Report{}, fmt.Errorf("send: %w", err)
		}
	case <-ctx.Done():
		h.Close()
		return h, Report{}, ctx.Err()
	}

	d := time.Since(start)
	rep := Report{
		Name:       opt.Name,
		Bytes:      recvN,
		Duration:   d,
		Throughput: float64(recvN) / d.Seconds(),
		Strategy:   opt.Scheduler,
	}
	if snap := client.Snapshot(); snap != nil {
		rep.Client = snap
		rep.Fallback = snap.Mode != "multipath"
	}
	if snap := sconn.Snapshot(); snap != nil {
		rep.Server = snap
	}
	return h, rep, nil
}
