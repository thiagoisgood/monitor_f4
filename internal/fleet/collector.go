package fleet

import (
	"bytes"
	"compress/gzip"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

//go:embed collector.py
var collectorScript string

//go:embed seed.json
var seed []byte

type Queryer interface {
	Query(context.Context, HostConfig, string) ([]byte, error)
	Close()
}
type Runner struct {
	cfg     Config
	mu      sync.Mutex
	clients map[string]*ssh.Client
	signer  ssh.Signer
	verify  ssh.HostKeyCallback
}

func NewRunner(c Config) (*Runner, error) {
	key, e := os.ReadFile(c.SSHKey)
	if e != nil {
		return nil, e
	}
	signer, e := ssh.ParsePrivateKey(key)
	if e != nil {
		return nil, e
	}
	verify, e := knownhosts.New(c.KnownHosts)
	if e != nil {
		return nil, e
	}
	return &Runner{cfg: c, clients: map[string]*ssh.Client{}, signer: signer, verify: verify}, nil
}

func (r *Runner) clientConfig(user string) *ssh.ClientConfig {
	// The existing hub trust file pins ED25519 keys for all three catalog hosts.
	// Match OpenSSH's known-key preference instead of negotiating an unpinned ECDSA key.
	return &ssh.ClientConfig{User: user, Auth: []ssh.AuthMethod{ssh.PublicKeys(r.signer)},
		HostKeyCallback: r.verify, HostKeyAlgorithms: []string{ssh.KeyAlgoED25519}, Timeout: 5 * time.Second}
}

type boundedBuffer struct {
	b bytes.Buffer
	n int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	size := len(p)
	if b.n > 0 {
		n := len(p)
		if n > b.n {
			n = b.n
		}
		b.b.Write(p[:n])
		b.n -= n
	}
	return size, nil
}

func unpackInventory(wire []byte) ([]byte, error) {
	reader, e := gzip.NewReader(bytes.NewReader(wire))
	if e != nil {
		return nil, fmt.Errorf("invalid_inventory_transport")
	}
	defer reader.Close()
	b, e := io.ReadAll(io.LimitReader(reader, 512*1024+1))
	if e != nil || len(b) > 512*1024 {
		return nil, fmt.Errorf("invalid_inventory_transport")
	}
	return b, nil
}
func (r *Runner) Query(ctx context.Context, h HostConfig, mode string) ([]byte, error) {
	if mode != "metrics" && mode != "services" && mode != "inventory" {
		return nil, fmt.Errorf("invalid collection mode")
	}
	if _, ok := hostConfig(h.ID); !ok {
		return nil, fmt.Errorf("unknown host")
	}
	out := &boundedBuffer{n: 512 * 1024}
	errout := &boundedBuffer{n: 1024}
	if h.Local {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(queryCtx, "/usr/bin/nice", "-n", "10", "/usr/bin/ionice", "-c", "3", "/usr/bin/python3", "-", mode, h.ID)
		cmd.Stdin = strings.NewReader(collectorScript)
		cmd.Stdout = out
		cmd.Stderr = errout
		e := cmd.Run()
		if e != nil {
			return nil, fmt.Errorf("local_query_failed")
		}
		return out.b.Bytes(), nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	client := r.clients[h.ID]
	if client == nil {
		// A first connection must not consume the subsequent collection's budget.
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		addr := net.JoinHostPort(h.Address, "22")
		conn, e := (&net.Dialer{}).DialContext(connectCtx, "tcp", addr)
		if e != nil {
			return nil, fmt.Errorf("ssh_connect_failed")
		}
		deadline, _ := connectCtx.Deadline()
		conn.SetDeadline(deadline)
		cc, ch, req, e := ssh.NewClientConn(conn, addr, r.clientConfig(h.User))
		if e != nil {
			conn.Close()
			return nil, fmt.Errorf("ssh_auth_or_hostkey_failed")
		}
		conn.SetDeadline(time.Time{})
		client = ssh.NewClient(cc, ch, req)
		r.clients[h.ID] = client
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		session, e := client.NewSession()
		if e != nil {
			done <- result{e}
			return
		}
		defer session.Close()
		session.Stdin = strings.NewReader(collectorScript)
		session.Stdout = out
		session.Stderr = errout
		command := "LC_ALL=C timeout 5s nice -n 10 ionice -c 3 python3 - " + mode + " " + h.ID
		if mode == "inventory" {
			command += " --gzip"
		}
		done <- result{session.Run(command)}
	}()
	select {
	case result := <-done:
		if result.err != nil {
			client.Close()
			delete(r.clients, h.ID)
			return nil, fmt.Errorf("ssh_query_failed")
		}
		if mode == "inventory" {
			return unpackInventory(out.b.Bytes())
		}
		return out.b.Bytes(), nil
	case <-queryCtx.Done():
		client.Close()
		delete(r.clients, h.ID)
		<-done
		return nil, fmt.Errorf("query_timeout")
	}
}
func (r *Runner) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.clients {
		c.Close()
	}
	r.clients = map[string]*ssh.Client{}
}

type rawMetrics struct {
	CPUTicks        []uint64   `json:"cpu_ticks"`
	MemoryTotal     uint64     `json:"memory_total"`
	MemoryAvailable uint64     `json:"memory_available"`
	SwapUsed        uint64     `json:"swap_used"`
	DiskPercent     float64    `json:"disk_percent"`
	DiskTotal       uint64     `json:"disk_total"`
	DiskFree        uint64     `json:"disk_free"`
	ExtraDisk       *ExtraDisk `json:"extra_disk,omitempty"`
	Load            []float64  `json:"load"`
	Uptime          float64    `json:"uptime"`
	Cores           int        `json:"cores"`
	RX              uint64     `json:"rx_total"`
	TX              uint64     `json:"tx_total"`
	BootID          string     `json:"boot_id"`
}
type rawSample struct {
	rawMetrics
	at time.Time
}

func calculate(current rawMetrics, previous *rawSample, now time.Time) (Metrics, error) {
	if current.MemoryTotal == 0 || current.MemoryAvailable > current.MemoryTotal || current.DiskTotal == 0 || len(current.CPUTicks) != 8 || current.Cores < 1 || len(current.Load) != 3 {
		return Metrics{}, fmt.Errorf("invalid_metrics")
	}
	v := Metrics{MemoryPercent: 100 * float64(current.MemoryTotal-current.MemoryAvailable) / float64(current.MemoryTotal), MemoryTotal: current.MemoryTotal, MemoryAvailable: current.MemoryAvailable, SwapUsed: current.SwapUsed, DiskPercent: current.DiskPercent, DiskTotal: current.DiskTotal, DiskFree: current.DiskFree, ExtraDisk: current.ExtraDisk, Load: current.Load, Uptime: current.Uptime, Cores: current.Cores, BootID: current.BootID}
	if previous != nil && previous.BootID == current.BootID && len(previous.CPUTicks) == 8 {
		var sum, idle uint64
		valid := true
		for i, n := range current.CPUTicks {
			if n < previous.CPUTicks[i] {
				valid = false
				break
			}
			d := n - previous.CPUTicks[i]
			sum += d
			if i == 3 || i == 4 {
				idle += d
			}
		}
		if valid && sum > 0 {
			v.CPU = number(100 * float64(sum-idle) / float64(sum))
		}
		seconds := now.Sub(previous.at).Seconds()
		if seconds > 0 && current.RX >= previous.RX && current.TX >= previous.TX {
			v.RX = number(float64(current.RX-previous.RX) / seconds)
			v.TX = number(float64(current.TX-previous.TX) / seconds)
		}
	}
	return v, nil
}
func initialHosts() []HostView {
	var data struct {
		Hosts []struct {
			ID           string     `json:"id"`
			OS           string     `json:"os"`
			Kernel       string     `json:"kernel"`
			Cores        int        `json:"cores"`
			Memory       float64    `json:"memory"`
			Disk         float64    `json:"disk"`
			Date         string     `json:"date"`
			SoftwareDate string     `json:"softwareDate"`
			Software     [][]string `json:"software"`
			Containers   [][]string `json:"containers"`
			Compose      [][]string `json:"compose"`
			Note         string     `json:"note"`
		}
	}
	if json.Unmarshal(seed, &data) != nil {
		panic("invalid embedded inventory")
	}
	out := []HostView{}
	for _, cfg := range Catalog {
		h := HostView{HostConfig: cfg, Connection: "pending", Metrics: MetricModule{Quality: Quality{State: "pending", Source: "collector", ExpectedSeconds: 30}}, Runtime: ServiceModule{Quality: Quality{State: "historical", Source: "inventory_snapshot", ExpectedSeconds: 60}, Containers: []Container{}, Services: []Service{}, Compose: []Compose{}, Probes: []Probe{}}}
		for _, s := range data.Hosts {
			if cfg.ID != s.ID {
				continue
			}
			h.Inventory = Inventory{Quality: Quality{State: "historical", Source: "inventory_snapshot", ObservedAt: s.Date, ExpectedSeconds: 86400}, OS: s.OS, Kernel: s.Kernel, Architecture: "x86_64", Cores: s.Cores, MemoryTotal: uint64(s.Memory * 1024 * 1024 * 1024), DiskTotal: uint64(s.Disk * 1024 * 1024 * 1024), Software: []Software{}, Note: s.Note}
			h.Runtime.Quality.ObservedAt = s.Date
			for _, row := range s.Software {
				if len(row) < 2 {
					continue
				}
				date := s.Date
				if s.ID == "t1" {
					date = "2026-10-20"
					if row[0] == "LiteLLM" || row[0] == "Claude Code" || row[0] == "PostgreSQL" {
						date = "2026-10-26"
					}
				}
				h.Inventory.Software = append(h.Inventory.Software, Software{row[0], row[1], "历史清单", date})
			}
			for _, c := range s.Containers {
				if len(c) < 4 {
					continue
				}
				h.Runtime.Containers = append(h.Runtime.Containers, Container{Name: c[0], Image: c[1], Status: c[2], Ports: c[3], State: "historical", Health: "unknown", ExpectedExit: c[0] == "example-init"})
			}
			for _, c := range s.Compose {
				if len(c) >= 3 {
					h.Runtime.Compose = append(h.Runtime.Compose, Compose{c[0], c[1], c[2]})
				}
			}
		}
		out = append(out, h)
	}
	return out
}

var _ io.Writer = (*boundedBuffer)(nil)
