package fleet

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type cpuReading struct {
	at    time.Time
	usage uint64
}
type Sampler struct {
	cfg          Config
	readings     []cpuReading
	cgroup       string
	lastHealth   int
	lastHealthMS float64
	lastProbe    time.Time
}

func NewSampler(cfg Config) *Sampler {
	s := &Sampler{cfg: cfg}
	b, _ := os.ReadFile("/proc/self/cgroup")
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "0::") {
			s.cgroup = filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::"))
		}
	}
	return s
}
func keyNumbers(path string) map[string]uint64 {
	out := map[string]uint64{}
	b, _ := os.ReadFile(path)
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(strings.ReplaceAll(line, ":", ""))
		if len(fields) >= 2 {
			v, e := strconv.ParseUint(fields[1], 10, 64)
			if e == nil {
				out[fields[0]] = v
			}
		}
	}
	return out
}
func readNumber(path string) uint64 {
	b, _ := os.ReadFile(path)
	v, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	return v
}
func (s *Sampler) Sample(now time.Time) Resource {
	if s.cfg.Fixture {
		return Resource{At: now, Memory: 32 * MiB, Available: 900 * MiB, DiskFree: 10 * 1024 * MiB, CPUPercent: .2, CPU15m: .2, HealthStatus: 200, Fixture: true}
	}
	mem := keyNumbers("/proc/meminfo")
	rss := keyNumbers("/proc/self/status")["VmRSS"] * 1024
	usage := uint64(0)
	oom := uint64(0)
	if s.cgroup != "" {
		if n := readNumber(filepath.Join(s.cgroup, "memory.current")); n > 0 {
			rss = n
		}
		usage = keyNumbers(filepath.Join(s.cgroup, "cpu.stat"))["usage_usec"]
		oom = keyNumbers(filepath.Join(s.cgroup, "memory.events"))["oom_kill"]
	}
	if usage == 0 {
		var own, children syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &own)
		syscall.Getrusage(syscall.RUSAGE_CHILDREN, &children)
		usage = uint64(own.Utime.Sec+own.Stime.Sec+children.Utime.Sec+children.Stime.Sec)*1000000 + uint64(own.Utime.Usec+own.Stime.Usec+children.Utime.Usec+children.Stime.Usec)
	}
	s.readings = append(s.readings, cpuReading{now, usage})
	for len(s.readings) > 2 && now.Sub(s.readings[1].at) > 15*time.Minute {
		s.readings = s.readings[1:]
	}
	cpu := 0.0
	average := 0.0
	if n := len(s.readings); n > 1 {
		a, b := s.readings[n-2], s.readings[n-1]
		if b.usage >= a.usage && b.at.After(a.at) {
			cpu = float64(b.usage-a.usage) / b.at.Sub(a.at).Seconds() / 10000
		}
		a = s.readings[0]
		if b.usage >= a.usage && b.at.After(a.at) {
			average = float64(b.usage-a.usage) / b.at.Sub(a.at).Seconds() / 10000
		}
	}
	if now.Sub(s.lastProbe) >= 30*time.Second {
		s.lastHealth, s.lastHealthMS = health()
		s.lastProbe = now
	}
	var fs syscall.Statfs_t
	syscall.Statfs(s.cfg.DataDir, &fs)
	pressure, _ := os.ReadFile("/proc/pressure/memory")
	ioPressure, _ := os.ReadFile("/proc/pressure/io")
	return Resource{At: now, Memory: rss, Available: mem["MemAvailable"] * 1024, DiskFree: uint64(fs.Bavail) * uint64(fs.Bsize), CPUPercent: cpu, CPU15m: average, HealthStatus: s.lastHealth, HealthMS: s.lastHealthMS, PressureMemory: strings.TrimSpace(string(pressure)), PressureIO: strings.TrimSpace(string(ioPressure)), OOM: oom}
}
func health() (int, float64) {
	start := time.Now()
	client := http.Client{Timeout: time.Second}
	response, e := client.Get("http://127.0.0.1:4000/health/liveliness")
	if e != nil {
		return 0, float64(time.Since(start).Microseconds()) / 1000
	}
	response.Body.Close()
	return response.StatusCode, float64(time.Since(start).Microseconds()) / 1000
}

type Guard struct {
	Low        int
	Critical   int
	Over       int
	Degraded   bool
	DegradedAt time.Time
	OverSince  time.Time
	BadHealth  int
}

func (g *Guard) Check(v Resource, now time.Time, cpuWindowReady bool) (bool, string) {
	if v.Available < 350*MiB {
		g.Low++
	} else {
		g.Low = 0
	}
	if v.Available < 250*MiB {
		g.Critical++
	} else {
		g.Critical = 0
	}
	over := v.Memory > 96*MiB || (cpuWindowReady && v.CPU15m > 2)
	if over {
		g.Over++
	} else {
		g.Over = 0
		g.OverSince = time.Time{}
	}
	if v.HealthStatus != 200 {
		g.BadHealth++
	} else {
		g.BadHealth = 0
	}
	if !g.Degraded && (g.Low >= 3 || g.Over >= 3) {
		g.Degraded = true
		g.DegradedAt = now
	}
	if g.Degraded && over {
		if g.OverSince.IsZero() {
			g.OverSince = now
		}
		if now.Sub(g.OverSince) >= 3*time.Minute {
			return true, "resource_budget_exceeded_after_degrade"
		}
	}
	if g.Critical >= 3 {
		return true, "host_available_memory_below_250MiB"
	}
	if v.OOM > 0 {
		return true, "monitor_cgroup_oom"
	}
	if g.BadHealth >= 9 {
		return true, "existing_gateway_unhealthy"
	}
	return false, ""
}
func Preflight(c Config) error {
	if c.Fixture {
		return nil
	}
	for i := 0; i <= 30; i++ {
		m := keyNumbers("/proc/meminfo")
		var fs syscall.Statfs_t
		if e := syscall.Statfs(c.DataDir, &fs); e != nil {
			return e
		}
		status, _ := health()
		if m["MemAvailable"]*1024 < 450*MiB {
			return fmt.Errorf("preflight: available memory below 450MiB")
		}
		if uint64(fs.Bavail)*uint64(fs.Bsize) < 5*1024*MiB {
			return fmt.Errorf("preflight: free disk below 5GiB")
		}
		if status != 200 {
			return fmt.Errorf("preflight: original gateway unhealthy")
		}
		if i < 30 {
			time.Sleep(10 * time.Second)
		}
	}
	return nil
}
func pauseMarker(dir, reason string) error {
	b, _ := json.MarshalIndent(map[string]any{"at": stamp(time.Now()), "reason": reason, "resume": "Inspect the cause, preserve evidence, remove this marker and manually start fleet-monitor.service"}, "", "  ")
	return os.WriteFile(filepath.Join(dir, "PAUSED.json"), b, 0600)
}
