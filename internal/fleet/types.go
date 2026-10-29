package fleet

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

const MiB = uint64(1024 * 1024)

type Config struct {
	Bind         string `json:"bind"`
	DataDir      string `json:"data_dir"`
	SSHKey       string `json:"ssh_key"`
	KnownHosts   string `json:"known_hosts"`
	Fixture      bool   `json:"fixture"`
	StageSeconds int    `json:"stage_seconds"`
	MaxHosts     int    `json:"max_hosts"`
	RolloutMode  string `json:"rollout_mode,omitempty"`
}

func ReadConfig(path string) (Config, error) {
	c := Config{Bind: "127.0.0.1:18090", DataDir: "/var/lib/fleet-monitor", SSHKey: "/home/monitor/.ssh/id_ed25519", KnownHosts: "/home/monitor/.ssh/known_hosts", StageSeconds: 900, MaxHosts: 4}
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return c, e
	}
	host, _, e := net.SplitHostPort(c.Bind)
	if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return c, fmt.Errorf("listener must be an explicit loopback IP")
	}
	if c.StageSeconds < 900 && !c.Fixture {
		return c, fmt.Errorf("production stages must last at least 900 seconds")
	}
	if c.DataDir == "" {
		return c, fmt.Errorf("data_dir required")
	}
	if c.MaxHosts < 1 || c.MaxHosts > len(Catalog) {
		return c, fmt.Errorf("max_hosts must be 1..4")
	}
	if c.RolloutMode != "" && c.RolloutMode != "staged" && c.RolloutMode != "manual" {
		return c, fmt.Errorf("rollout_mode must be staged or manual")
	}
	return c, nil
}

type HostConfig struct {
	ID         string `json:"id"`
	Role       string `json:"role"`
	Address    string `json:"address"`
	User       string `json:"user"`
	Local      bool   `json:"-"`
	SudoDocker bool   `json:"-"`
}

var Catalog = []HostConfig{
	{"t1", "示例主机 1", "192.0.2.1", "monitor", true, false},
	{"t2", "示例主机 2", "192.0.2.2", "monitor", false, true},
	{"t3", "示例主机 3", "192.0.2.3", "monitor", false, false},
	{"ali_cjlass", "示例主机 4", "192.0.2.4", "monitor", false, false},
}

func hostConfig(id string) (HostConfig, bool) {
	for _, h := range Catalog {
		if h.ID == id {
			return h, true
		}
	}
	return HostConfig{}, false
}

type Quality struct {
	State           string `json:"state"`
	Source          string `json:"source"`
	ObservedAt      string `json:"observed_at,omitempty"`
	LastSuccess     string `json:"last_success,omitempty"`
	LastAttempt     string `json:"last_attempt,omitempty"`
	ExpectedSeconds int    `json:"expected_seconds"`
	Error           string `json:"error,omitempty"`
}

func (q Quality) fresh(now time.Time) Quality {
	if q.State == "live" || q.State == "partial" || q.State == "demo" {
		t, e := time.Parse(time.RFC3339Nano, q.LastSuccess)
		if e != nil || now.Sub(t) > time.Duration(3*q.ExpectedSeconds)*time.Second {
			q.State = "stale"
		}
	}
	return q
}

type ExtraDisk struct {
	Mount   string  `json:"mount"`
	State   string  `json:"state"`
	Total   uint64  `json:"total,omitempty"`
	Free    uint64  `json:"free,omitempty"`
	Percent float64 `json:"percent"`
}

type Metrics struct {
	CPU             *float64   `json:"cpu"`
	MemoryPercent   float64    `json:"memory_percent"`
	MemoryTotal     uint64     `json:"memory_total"`
	MemoryAvailable uint64     `json:"memory_available"`
	SwapUsed        uint64     `json:"swap_used"`
	DiskPercent     float64    `json:"disk_percent"`
	DiskTotal       uint64     `json:"disk_total"`
	DiskFree        uint64     `json:"disk_free"`
	ExtraDisk       *ExtraDisk `json:"extra_disk,omitempty"`
	Load            []float64  `json:"load"`
	Uptime          float64    `json:"uptime"`
	RX              *float64   `json:"rx_bytes_sec"`
	TX              *float64   `json:"tx_bytes_sec"`
	Cores           int        `json:"cores"`
	BootID          string     `json:"-"`
}
type MetricModule struct {
	Quality Quality  `json:"quality"`
	Values  *Metrics `json:"values"`
}
type Container struct {
	Name         string `json:"name"`
	Image        string `json:"image"`
	State        string `json:"state"`
	Status       string `json:"status"`
	Health       string `json:"health"`
	Ports        string `json:"ports"`
	ExpectedExit bool   `json:"expected_exit"`
}
type Service struct {
	Name     string `json:"name"`
	Scope    string `json:"scope"`
	State    string `json:"state"`
	SubState string `json:"sub_state"`
}
type Compose struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Path   string `json:"path"`
}
type Probe struct {
	Name      string  `json:"name"`
	Status    int     `json:"status"`
	State     string  `json:"state"`
	LatencyMS float64 `json:"latency_ms"`
}
type ServiceModule struct {
	Quality      Quality     `json:"quality"`
	Containers   []Container `json:"containers"`
	Services     []Service   `json:"services"`
	Compose      []Compose   `json:"compose"`
	Probes       []Probe     `json:"probes"`
	DockerError  string      `json:"docker_error,omitempty"`
	ComposeError string      `json:"compose_error,omitempty"`
}
type Software struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	Source     string `json:"source"`
	ObservedAt string `json:"observed_at"`
}
type Inventory struct {
	Quality      Quality    `json:"quality"`
	OS           string     `json:"os"`
	Kernel       string     `json:"kernel"`
	Architecture string     `json:"architecture"`
	Cores        int        `json:"cores"`
	MemoryTotal  uint64     `json:"memory_total"`
	DiskTotal    uint64     `json:"disk_total"`
	Software     []Software `json:"software"`
	Packages     []Software `json:"packages,omitempty"`
	PackageCount int        `json:"package_count"`
	PackagePage  int        `json:"package_page,omitempty"`
	Note         string     `json:"note"`
	Errors       []string   `json:"errors,omitempty"`
}
type HostView struct {
	HostConfig
	Enabled      bool          `json:"enabled"`
	Connection   string        `json:"connection"`
	Metrics      MetricModule  `json:"metrics"`
	Runtime      ServiceModule `json:"runtime"`
	Inventory    Inventory     `json:"inventory"`
	Failures     int           `json:"failures"`
	CollectionMS float64       `json:"collection_ms"`
}
type Event struct {
	ID         int64  `json:"id"`
	Key        string `json:"key"`
	Host       string `json:"host"`
	Severity   string `json:"severity"`
	Title      string `json:"title"`
	Detail     string `json:"detail"`
	State      string `json:"state"`
	FirstAt    string `json:"first_at"`
	LastAt     string `json:"last_at"`
	ResolvedAt string `json:"resolved_at,omitempty"`
}
type Dependency struct {
	ID         string `json:"id"`
	From       string `json:"from"`
	To         string `json:"to"`
	Label      string `json:"label"`
	State      string `json:"state"`
	Detail     string `json:"detail"`
	ObservedAt string `json:"observed_at,omitempty"`
}
type Rollout struct {
	Enabled        int        `json:"enabled"`
	MaxHosts       int        `json:"max_hosts"`
	PhaseStarted   time.Time  `json:"phase_started"`
	AllEnabledAt   *time.Time `json:"all_enabled_at,omitempty"`
	ObservationDue *time.Time `json:"observation_due,omitempty"`
	State          string     `json:"state"`
	Reason         string     `json:"reason,omitempty"`
	StageSeconds   int        `json:"stage_seconds"`
}
type Resource struct {
	At             time.Time `json:"at"`
	Memory         uint64    `json:"memory_bytes"`
	Available      uint64    `json:"available_bytes"`
	DiskFree       uint64    `json:"disk_free_bytes"`
	CPUPercent     float64   `json:"cpu_percent_one_core"`
	CPU15m         float64   `json:"cpu_15m_percent_one_core"`
	HealthStatus   int       `json:"litellm_health_status"`
	HealthMS       float64   `json:"litellm_health_ms"`
	PressureMemory string    `json:"memory_pressure"`
	PressureIO     string    `json:"io_pressure"`
	OOM            uint64    `json:"oom_kills"`
	Fixture        bool      `json:"fixture"`
}
type Overview struct {
	Version      string       `json:"version"`
	Fixture      bool         `json:"fixture"`
	GeneratedAt  string       `json:"generated_at"`
	Hosts        []HostView   `json:"hosts"`
	Dependencies []Dependency `json:"dependencies"`
	Events       []Event      `json:"events"`
	Rollout      Rollout      `json:"rollout"`
	Resource     Resource     `json:"resource"`
	Degraded     bool         `json:"degraded"`
}

func stamp(t time.Time) string  { return t.UTC().Format(time.RFC3339Nano) }
func number(v float64) *float64 { return &v }
