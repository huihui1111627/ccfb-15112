package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"clocktower/multipath/internal/dashboard"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "dashboard listen address")
	sizeMB := flag.Int("size-mb", 32, "demo transfer size in MiB")
	loop := flag.Bool("loop", true, "continuously run alternating scheduler demos")
	flag.Parse()

	mgr := dashboard.NewManager()
	if *loop {
		mgr.StartLoop(*sizeMB)
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           dashboard.NewServer(mgr).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("clocktower multipath dashboard: http://%s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	mgr.StopLoop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
