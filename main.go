package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"local/fleet-monitor/internal/fleet"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

//go:embed web/dist
var assets embed.FS
var version = "dev"

type rotatingLog struct {
	mu   sync.Mutex
	path string
}

func (l *rotatingLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if s, e := os.Stat(l.path); e == nil && s.Size()+int64(len(p)) > 5*1024*1024 {
		os.Remove(l.path + ".5")
		for i := 4; i >= 1; i-- {
			os.Rename(fmt.Sprintf("%s.%d", l.path, i), fmt.Sprintf("%s.%d", l.path, i+1))
		}
		os.Rename(l.path, l.path+".1")
	}
	f, e := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	return f.Write(p)
}
func run() error {
	mode := "serve"
	args := os.Args[1:]
	if len(args) > 0 && (args[0] == "preflight" || args[0] == "version" || args[0] == "check-ssh") {
		mode = args[0]
		args = args[1:]
	}
	if mode == "version" {
		fmt.Println(version)
		return nil
	}
	flags := flag.NewFlagSet("fleet-monitor", flag.ContinueOnError)
	path := flags.String("config", "/opt/fleet-monitor/config.json", "config file")
	if e := flags.Parse(args); e != nil {
		return e
	}
	cfg, e := fleet.ReadConfig(*path)
	if e != nil {
		return e
	}
	if mode == "preflight" {
		return fleet.Preflight(cfg)
	}
	if mode == "check-ssh" {
		runner, e := fleet.NewRunner(cfg)
		if e != nil {
			return fmt.Errorf("SSH credential/hostkey initialization failed")
		}
		defer runner.Close()
		for _, host := range fleet.Catalog {
			if host.Local {
				continue
			}
			for _, module := range []string{"metrics", "services", "inventory"} {
				started := time.Now()
				b, e := runner.Query(context.Background(), host, module)
				if e != nil {
					return fmt.Errorf("%s/%s: %w", host.ID, module, e)
				}
				fmt.Printf("%s/%s ok (%d bytes, %.3fs)\n", host.ID, module, len(b), time.Since(started).Seconds())
			}
		}
		return nil
	}
	if e = os.MkdirAll(cfg.DataDir, 0700); e != nil {
		return e
	}
	log.SetOutput(&rotatingLog{path: filepath.Join(cfg.DataDir, "monitor.log")})
	var runner fleet.Queryer
	if !cfg.Fixture {
		runner, e = fleet.NewRunner(cfg)
		if e != nil {
			return fmt.Errorf("SSH credential/hostkey initialization failed")
		}
	}
	app, e := fleet.NewApp(cfg, version, runner)
	if e != nil {
		return e
	}
	defer app.Close()
	if cfg.Fixture {
		if e = app.SeedFixtureHistory(); e != nil {
			return e
		}
	}
	ui, e := fs.Sub(assets, "web/dist")
	if e != nil {
		return e
	}
	server := &http.Server{Addr: cfg.Bind, Handler: app.Handler(ui), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 45 * time.Second, MaxHeaderBytes: 8192}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan error, 2)
	appStopped := make(chan struct{})
	go func() { defer close(appStopped); done <- app.Run(ctx) }()
	go func() {
		e := server.ListenAndServe()
		if e == http.ErrServerClosed {
			e = nil
		}
		done <- e
	}()
	log.Printf("started version=%s bind=%s fixture=%t", version, cfg.Bind, cfg.Fixture)
	select {
	case <-ctx.Done():
	case e = <-done:
	}
	cancel()
	shutdown, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	server.Shutdown(shutdown)
	<-appStopped
	return e
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
