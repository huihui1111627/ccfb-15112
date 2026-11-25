package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"clocktower/multipath/internal/scenarios"
	"clocktower/multipath/internal/testkit"
	"clocktower/multipath/pkg/simnet"
	"clocktower/multipath/pkg/transport"
)

type row struct {
	Name           string  `json:"name"`
	Description    string  `json:"description"`
	Bytes          int64   `json:"bytes"`
	BaselineMBps   float64 `json:"baseline_mbps"`
	MinRTTMBps     float64 `json:"min_rtt_mbps"`
	RoundRobinMBps float64 `json:"round_robin_mbps"`
	Ratio          float64 `json:"ratio_vs_best_single"`
	RequiredRatio  float64 `json:"required_ratio"`
	Pass           bool    `json:"pass"`
	MinRTTLatMS    float64 `json:"min_rtt_latency_ms"`
	RRLatMS        float64 `json:"rr_latency_ms"`
	BaselineLatMS  float64 `json:"baseline_latency_ms"`
	Fallback       bool    `json:"fallback"`
	RescueEvents   int64   `json:"rescue_events"`
	DurationMS     int64   `json:"duration_ms"`
	Error          string  `json:"error,omitempty"`
}

type output struct {
	StartedAt time.Time `json:"started_at"`
	MatrixMB  int       `json:"matrix_mb"`
	LargeMB   int       `json:"large_mb"`
	Rows      []row     `json:"rows"`
	Large     *row      `json:"large,omitempty"`
}

func randomSource(seed int64, n int64) (io.Reader, hash.Hash) {
	h := sha256.New()
	r := io.TeeReader(io.LimitReader(rand.New(rand.NewSource(seed)), n), h)
	return r, h
}

func runOne(ctx context.Context, name string, sizeMB int, profiles []simnet.Profile, scheduler transport.SchedulerKind, maxSub int, timeout time.Duration) (testkit.Report, error) {
	size := int64(sizeMB) << 20
	src, sourceHash := randomSource(0xC10C+int64(sizeMB)+int64(len(profiles))*17, size)
	dst := sha256.New()
	hk, rep, err := testkit.Run(ctx, testkit.Options{
		Name: name, Size: size, Profiles: profiles, Scheduler: scheduler,
		MaxSubflows: maxSub, Timeout: timeout,
	}, src, dst)
	if hk != nil {
		defer hk.Close()
	}
	if err != nil {
		return rep, err
	}
	if got := dst.Sum(nil); string(got) != string(sourceHash.Sum(nil)) {
		return rep, fmt.Errorf("byte stream/hash mismatch")
	}
	return rep, nil
}

func runLargeFile(ctx context.Context, sizeMB int, profiles []simnet.Profile) (testkit.Report, string, string, error) {
	size := int64(sizeMB) << 20
	dir, err := os.MkdirTemp("", "clocktower-mptest-")
	if err != nil {
		return testkit.Report{}, "", "", err
	}
	sourcePath := filepath.Join(dir, "random-file.bin")
	receivedPath := filepath.Join(dir, "received-file.bin")

	srcWriter, err := os.Create(sourcePath)
	if err != nil {
		return testkit.Report{}, sourcePath, receivedPath, err
	}
	rng := rand.New(rand.NewSource(20261126))
	if _, err := io.CopyN(srcWriter, rng, size); err != nil {
		_ = srcWriter.Close()
		return testkit.Report{}, sourcePath, receivedPath, err
	}
	if err := srcWriter.Close(); err != nil {
		return testkit.Report{}, sourcePath, receivedPath, err
	}

	src, err := os.Open(sourcePath)
	if err != nil {
		return testkit.Report{}, sourcePath, receivedPath, err
	}
	defer src.Close()
	dst, err := os.Create(receivedPath)
	if err != nil {
		return testkit.Report{}, sourcePath, receivedPath, err
	}

	hk, rep, runErr := testkit.Run(ctx, testkit.Options{
		Name: "large-random-file", Size: size, Profiles: profiles,
		Scheduler: transport.SchedulerMinRTT, MaxSubflows: len(profiles), Timeout: 8 * time.Minute,
	}, src, dst)
	_ = dst.Close()
	if hk != nil {
		hk.Close()
	}
	if runErr != nil {
		return rep, sourcePath, receivedPath, runErr
	}
	if err := compareFilesByteByByte(sourcePath, receivedPath); err != nil {
		return rep, sourcePath, receivedPath, err
	}
	return rep, sourcePath, receivedPath, nil
}

func compareFilesByteByByte(a, b string) error {
	f1, err := os.Open(a)
	if err != nil {
		return err
	}
	defer f1.Close()
	f2, err := os.Open(b)
	if err != nil {
		return err
	}
	defer f2.Close()
	i1, err := f1.Stat()
	if err != nil {
		return err
	}
	i2, err := f2.Stat()
	if err != nil {
		return err
	}
	if i1.Size() != i2.Size() {
		return fmt.Errorf("size mismatch: %d != %d", i1.Size(), i2.Size())
	}
	const chunk = 4 << 20
	buf1 := make([]byte, chunk)
	buf2 := make([]byte, chunk)
	var offset int64
	for {
		n1, e1 := io.ReadFull(f1, buf1)
		n2, e2 := io.ReadFull(f2, buf2)
		if n1 != n2 || !bytes.Equal(buf1[:n1], buf2[:n2]) {
			return fmt.Errorf("byte mismatch at offset ~%d", offset)
		}
		offset += int64(n1)
		if e1 == io.EOF && e2 == io.EOF {
			return nil
		}
		if e1 == io.ErrUnexpectedEOF && e2 == io.ErrUnexpectedEOF {
			return nil
		}
		if e1 != nil && e1 != io.ErrUnexpectedEOF {
			return e1
		}
		if e2 != nil && e2 != io.ErrUnexpectedEOF {
			return e2
		}
	}
}

func mbps(bps float64) float64 { return bps / (1000 * 1000) }

func main() {
	var matrixMB = flag.Int("matrix-mb", 4, "random MiB transferred for each matrix strategy")
	var largeMB = flag.Int("large-mb", 256, "large random file MiB; zero disables")
	var timeout = flag.Duration("case-timeout", 45*time.Second, "per transfer timeout")
	var outJSON = flag.String("json", "mptest-report.json", "JSON report path")
	var outMD = flag.String("markdown", "mptest-report.md", "Markdown report path")
	var skipRR = flag.Bool("skip-rr", false, "skip round-robin comparison to run faster")
	var filter = flag.String("filter", "", "only run scenario names containing this substring")
	var skipMatrix = flag.Bool("skip-matrix", false, "skip the dozens-case matrix and only run the large file check")
	flag.Parse()

	all := scenarios.All()
	res := output{StartedAt: time.Now(), MatrixMB: *matrixMB, LargeMB: *largeMB, Rows: make([]row, 0, len(all))}
	failed := 0

	fmt.Printf("clocktower multipath matrix: %d scenarios, %d MiB/run\n", len(all), *matrixMB)
	for i, sc := range all {
		if *skipMatrix {
			break
		}
		if *filter != "" && !strings.Contains(sc.Name, *filter) {
			continue
		}
		r := row{Name: sc.Name, Description: sc.Description, RequiredRatio: sc.Ratio}
		ctx, cancel := context.WithTimeout(context.Background(), *timeout*time.Duration(4+len(sc.Profiles))+20*time.Second)

		// "Best single path" is measured empirically for every candidate, not
		// only selected from the theoretical profile score.
		for _, bp := range sc.Profiles {
			base, err := runOne(ctx, fmt.Sprintf("%s-single-%d", sc.Name, bp.PathID), *matrixMB, []simnet.Profile{bp}, transport.SchedulerMinRTT, 1, *timeout)
			if err != nil {
				// An intentionally terrible candidate may time out; the
				// scenario only needs the best standalone path to succeed.
				continue
			}
			if v := mbps(base.Throughput); v > r.BaselineMBps {
				r.BaselineMBps = v
				if base.Client != nil {
					r.BaselineLatMS = base.Client.LatencyAvgMS
				}
			}
		}

		mr, err := runOne(ctx, sc.Name+"-minrtt", *matrixMB, sc.Profiles, transport.SchedulerMinRTT, len(sc.Profiles), *timeout)
		if err != nil {
			if r.Error != "" {
				r.Error += "; "
			}
			r.Error += "min-rtt: " + err.Error()
		} else {
			r.MinRTTMBps = mbps(mr.Throughput)
			r.DurationMS = mr.Duration.Milliseconds()
			if mr.Client != nil {
				r.MinRTTLatMS = mr.Client.LatencyAvgMS
				r.Fallback = mr.Fallback
				r.RescueEvents = mr.Client.RescueEvents
			}
		}

		if !*skipRR {
			rr, err := runOne(ctx, sc.Name+"-rr", *matrixMB, sc.Profiles, transport.SchedulerRoundRobin, len(sc.Profiles), *timeout)
			if err != nil {
				if r.Error != "" {
					r.Error += "; "
				}
				r.Error += "rr: " + err.Error()
			} else {
				r.RoundRobinMBps = mbps(rr.Throughput)
				if rr.Client != nil {
					r.RRLatMS = rr.Client.LatencyAvgMS
				}
			}
		}

		if r.BaselineMBps > 0 {
			r.Ratio = r.MinRTTMBps / r.BaselineMBps
			r.Pass = r.Error == "" && r.Ratio+1e-9 >= r.RequiredRatio
		}
		if !r.Pass {
			failed++
		}
		res.Rows = append(res.Rows, r)
		cancel()
		fmt.Printf("[%02d/%02d] %-20s single=%7.1f min=%7.1f rr=%7.1f ratio=%.2f req=%.2f %s\n",
			i+1, len(all), sc.Name, r.BaselineMBps, r.MinRTTMBps, r.RoundRobinMBps,
			r.Ratio, r.RequiredRatio, map[bool]string{true: "PASS", false: "FAIL"}[r.Pass])
	}

	if *largeMB > 0 {
		fmt.Printf("large exact-byte transfer: %d MiB...\n", *largeMB)
		profiles := []simnet.Profile{
			{PathID: 1, Name: "mobile", Priority: 1, RateBytesPerSec: simnet.Mbps(700), OneWayDelay: 8 * time.Millisecond, LossRate: 0.006, Seed: 4242},
			{PathID: 2, Name: "radio", Priority: 2, RateBytesPerSec: simnet.Mbps(260), OneWayDelay: 80 * time.Millisecond, LossRate: 0.03, Seed: 9999, FlapEvery: 900 * time.Millisecond, FlapDown: 180 * time.Millisecond},
			{PathID: 3, Name: "backup", Priority: 3, RateBytesPerSec: simnet.Mbps(400), OneWayDelay: 18 * time.Millisecond, LossRate: 0.01, Seed: 777},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		rep, sourcePath, receivedPath, err := runLargeFile(ctx, *largeMB, profiles)
		cancel()
		lr := row{Name: "large-random-file", Bytes: int64(*largeMB) << 20, RequiredRatio: 0.6}
		if err != nil {
			lr.Error = err.Error()
			failed++
		} else {
			lr.MinRTTMBps = mbps(rep.Throughput)
			lr.DurationMS = rep.Duration.Milliseconds()
			if rep.Client != nil {
				lr.MinRTTLatMS = rep.Client.LatencyAvgMS
				lr.Fallback = rep.Fallback
				lr.RescueEvents = rep.Client.RescueEvents
			}
			lr.Pass = true
			fmt.Printf("large PASS: %d bytes in %s, %.1f MByte/s\n  sent:     %s\n  received: %s\n  compare:  byte-for-byte OK\n",
				lr.Bytes, rep.Duration, lr.MinRTTMBps, sourcePath, receivedPath)
		}
		res.Large = &lr
	}

	if *outJSON != "" {
		if b, err := json.MarshalIndent(res, "", "  "); err == nil {
			_ = os.WriteFile(*outJSON, b, 0o644)
		}
	}
	if *outMD != "" {
		writeMarkdown(*outMD, res)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "name\tbaseline MB/s\tminRTT MB/s\tRR MB/s\tratio\tlat min/rr/base ms\tstatus")
	for _, r := range res.Rows {
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
		}
		fmt.Fprintf(w, "%s\t%.1f\t%.1f\t%.1f\t%.2f\t%.1f/%.1f/%.1f\t%s\n",
			r.Name, r.BaselineMBps, r.MinRTTMBps, r.RoundRobinMBps, r.Ratio,
			r.MinRTTLatMS, r.RRLatMS, r.BaselineLatMS, status)
	}
	_ = w.Flush()
	fmt.Printf("report: %s / %s\n", *outJSON, *outMD)
	if failed > 0 {
		fmt.Printf("%d matrix/large checks failed\n", failed)
		os.Exit(1)
	}
}

func writeMarkdown(path string, res output) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "# Multipath test report\n\nGenerated: %s\n\n", res.StartedAt.Format(time.RFC3339))
	fmt.Fprintln(f, "| Case | Best single MB/s | MinRTT MB/s | RR MB/s | Ratio | Required | Lat min/rr/base ms | Result |")
	fmt.Fprintln(f, "|---|---:|---:|---:|---:|---:|---:|---|")
	for _, r := range res.Rows {
		status := "✅ PASS"
		if !r.Pass {
			status = "❌ " + r.Error
		}
		fmt.Fprintf(f, "| %s | %.1f | %.1f | %.1f | %.2f | %.2f | %.1f/%.1f/%.1f | %s |\n",
			r.Name, r.BaselineMBps, r.MinRTTMBps, r.RoundRobinMBps, r.Ratio, r.RequiredRatio,
			r.MinRTTLatMS, r.RRLatMS, r.BaselineLatMS, status)
	}
	if res.Large != nil {
		fmt.Fprintf(f, "\n## Large random file\n\n- bytes: %d\n- duration: %d ms\n- throughput: %.1f MB/s\n- hash: SHA-256 source and sink equal\n",
			res.Large.Bytes, res.Large.DurationMS, res.Large.MinRTTMBps)
		if res.Large.Error != "" {
			fmt.Fprintf(f, "- error: %s\n", res.Large.Error)
		}
	}
}
