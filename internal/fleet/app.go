package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type App struct {
	cfg            Config
	version        string
	store          *Store
	runner         Queryer
	sampler        *Sampler
	sampleResource func(time.Time) Resource
	rules          *Rules
	mu             sync.RWMutex
	hosts          []HostView
	previous       map[string]*rawSample
	next           map[string]time.Time
	successes      map[string]int
	guard          Guard
	resource       Resource
	rollout        Rollout
	started        time.Time
	cache          []byte
	stop           chan string
	once           sync.Once
}

func NewApp(c Config, version string, runner Queryer) (*App, error) {
	if c.MaxHosts == 0 {
		c.MaxHosts = len(Catalog)
	}
	if e := os.MkdirAll(c.DataDir, 0700); e != nil {
		return nil, e
	}
	if _, e := os.Stat(filepath.Join(c.DataDir, "PAUSED.json")); e == nil {
		return nil, fmt.Errorf("monitor suspended; inspect PAUSED.json before manually resuming")
	}
	s, e := OpenStore(c.DataDir)
	if e != nil {
		return nil, e
	}
	now := time.Now()
	a := &App{cfg: c, version: version, store: s, runner: runner, sampler: NewSampler(c), hosts: initialHosts(), previous: map[string]*rawSample{}, next: map[string]time.Time{}, successes: map[string]int{}, started: now, stop: make(chan string, 1)}
	a.rules = NewRules(s)
	a.sampleResource = a.sampler.Sample
	a.rollout = Rollout{Enabled: 1, PhaseStarted: now, State: "canary", StageSeconds: c.StageSeconds}
	s.load("state", "rollout", &a.rollout)
	a.rollout.PhaseStarted = now
	a.rollout.StageSeconds = c.StageSeconds
	a.rollout.MaxHosts = c.MaxHosts
	if a.rollout.State == "suspended" {
		a.rollout.State = "canary"
		a.rollout.Reason = "保护标记已人工移除，重新开始本阶段观察"
	}
	if a.rollout.Enabled > c.MaxHosts {
		a.rollout.Enabled = c.MaxHosts
		a.rollout.AllEnabledAt = nil
		a.rollout.ObservationDue = nil
		a.rollout.State = "canary"
	}
	if c.Fixture {
		a.rollout.Enabled = 4
		a.rollout.State = "demo"
	} else if c.RolloutMode == "manual" {
		a.rollout.Enabled = c.MaxHosts
		a.rollout.State = "awaiting_observation"
		a.rollout.Reason = "主机采集已启用，观察计时尚未启动"
		a.rollout.AllEnabledAt = nil
		a.rollout.ObservationDue = nil
	} else if a.rollout.State == "awaiting_observation" {
		a.rollout.State = "canary"
		a.rollout.Reason = ""
		if a.rollout.Enabled == len(Catalog) {
			due := now.Add(24 * time.Hour)
			a.rollout.AllEnabledAt = &now
			a.rollout.ObservationDue = &due
			a.rollout.State = "observing_24h"
		}
	}
	if e := s.save("state", "rollout", a.rollout); e != nil {
		s.Close()
		return nil, e
	}
	for i := range a.hosts {
		h := &a.hosts[i]
		s.load("inventory", h.ID, &h.Inventory)
		s.load("runtime", h.ID, &h.Runtime)
		h.Enabled = i < a.rollout.Enabled
		for _, mode := range []string{"metrics", "services", "inventory"} {
			a.next[h.ID+":"+mode] = now.Add(time.Duration(i*3) * time.Second)
		}
	}
	a.resource = a.sampler.Sample(now)
	a.refresh(now)
	return a, nil
}
func (a *App) Close() {
	if a.runner != nil {
		a.runner.Close()
	}
	a.store.Close()
}
func (a *App) fatal(reason string) {
	a.once.Do(func() {
		pauseMarker(a.cfg.DataDir, reason)
		log.Printf("monitor suspended: %s", reason)
		a.stop <- reason
	})
}
func (a *App) saveError(e error) {
	if e != nil {
		a.fatal("storage_write_failed")
	}
}
func (a *App) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	nextGuard := time.Time{}
	nextCleanup := time.Now().Add(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return nil
		case reason := <-a.stop:
			return fmt.Errorf("suspended: %s", reason)
		case now := <-ticker.C:
			if !now.Before(nextGuard) {
				a.checkGuard(now)
				nextGuard = now.Add(10 * time.Second)
			}
			if !now.Before(nextCleanup) {
				a.saveError(a.store.cleanup(now))
				nextCleanup = now.Add(time.Minute)
			}
			if a.hasStop() {
				continue
			}
			a.collectNext(ctx, now)
		}
	}
}
func (a *App) hasStop() bool {
	select {
	case reason := <-a.stop:
		a.stop <- reason
		return true
	default:
		return false
	}
}
func (a *App) checkGuard(now time.Time) {
	v := a.sampleResource(now)
	a.mu.Lock()
	a.resource = v
	defer a.mu.Unlock()
	stop, reason := a.guard.Check(v, now, now.Sub(a.started) >= 15*time.Minute)
	if v.DiskFree < 2*1024*MiB {
		a.store.cleanup(now)
		stop = true
		reason = "host_disk_free_below_2GiB"
	}
	if st, e := os.Stat(filepath.Join(a.cfg.DataDir, "fleet.db")); e == nil && st.Size() >= 512*1024*1024 {
		a.store.cleanup(now)
		stop = true
		reason = "database_limit_reached"
	}
	if stop {
		a.rollout.State = "suspended"
		a.rollout.Reason = reason
		a.store.save("state", "rollout", a.rollout)
		a.fatal(reason)
		a.refresh(now)
		return
	}
	a.saveError(a.store.resource(v))
	for i := range a.hosts {
		h := &a.hosts[i]
		if !h.Enabled {
			continue
		}
		q := h.Metrics.Quality.fresh(now)
		if q.State == "stale" {
			a.rules.Invalidate(h.ID)
			a.store.event(h.ID+":stale", h.ID, "warning", "采集数据过期", "数据超过三个有效采集周期，主机是否故障尚未知", now)
		}
	}
	a.advance(now)
	a.refresh(now)
}
func (a *App) advance(now time.Time) {
	if a.cfg.Fixture || a.cfg.RolloutMode == "manual" {
		return
	}
	healthy := !a.guard.Degraded && a.resource.Memory <= 96*MiB && a.resource.CPU15m <= 2 && a.resource.HealthStatus == 200
	for _, h := range a.hosts {
		if !h.Enabled {
			continue
		}
		m, q := h.Metrics.Quality.fresh(now), h.Runtime.Quality.fresh(now)
		if m.State != "live" || q.State != "live" || h.Runtime.DockerError != "" {
			healthy = false
		}
	}
	if !healthy {
		a.rollout.PhaseStarted = now
		a.rollout.Reason = "等待所有已启用主机采集正常且资源处于预算内"
		a.saveError(a.store.save("state", "rollout", a.rollout))
		return
	}
	a.rollout.Reason = ""
	if now.Sub(a.rollout.PhaseStarted) < time.Duration(a.cfg.StageSeconds)*time.Second {
		return
	}
	if a.rollout.Enabled >= a.cfg.MaxHosts && a.cfg.MaxHosts < len(Catalog) {
		a.rollout.Reason = "当前阶段已完成；主机上限限制继续扩展，需后续明确调整配置"
		if a.rollout.State != "stage_complete" {
			a.rollout.State = "stage_complete"
			a.saveError(a.store.save("state", "rollout", a.rollout))
		}
		return
	}
	if a.rollout.Enabled < 4 {
		a.rollout.Enabled++
		a.hosts[a.rollout.Enabled-1].Enabled = true
		a.rollout.PhaseStarted = now
		a.store.info("rollout", a.hosts[a.rollout.Enabled-1].ID, "启用下一台主机采集", "前一阶段已连续观察至少15分钟并保持预算内", now)
		if a.rollout.Enabled == 4 {
			t := now
			due := now.Add(24 * time.Hour)
			a.rollout.AllEnabledAt = &t
			a.rollout.ObservationDue = &due
			a.rollout.State = "four_host_canary"
		}
	} else {
		a.rollout.State = "observing_24h"
		if a.rollout.ObservationDue != nil && !now.Before(*a.rollout.ObservationDue) {
			a.rollout.State = "observation_window_elapsed"
		}
	}
	a.saveError(a.store.save("state", "rollout", a.rollout))
}
func (a *App) collectNext(ctx context.Context, now time.Time) {
	interval := 30 * time.Second
	a.mu.RLock()
	degraded := a.guard.Degraded
	a.mu.RUnlock()
	if degraded {
		interval = 60 * time.Second
	}
	for i := range a.hosts {
		a.mu.RLock()
		h := a.hosts[i]
		a.mu.RUnlock()
		if !h.Enabled {
			continue
		}
		for _, mode := range []string{"metrics", "services", "inventory"} {
			if mode == "inventory" && degraded {
				continue
			}
			key := h.ID + ":" + mode
			if now.Before(a.next[key]) {
				continue
			}
			period := interval
			if mode == "services" {
				period = interval * 2
			}
			if mode == "inventory" {
				period = 24 * time.Hour
			}
			start := time.Now()
			var b []byte
			var e error
			if a.cfg.Fixture {
				b = a.fixture(h, mode, now)
			} else {
				b, e = a.runner.Query(ctx, h.HostConfig, mode)
			}
			a.next[key] = time.Now().Add(period)
			if e != nil {
				backoff := h.Failures + 1
				if backoff > 6 {
					backoff = 6
				}
				a.next[key] = time.Now().Add(time.Duration(backoff) * period)
			}
			a.mu.Lock()
			target := &a.hosts[i]
			target.CollectionMS = float64(time.Since(start).Microseconds()) / 1000
			a.apply(target, mode, b, e, now, int(period.Seconds()))
			a.refresh(time.Now())
			a.mu.Unlock()
			return
		}
	}
}
func (a *App) quality(previous Quality, err error, now time.Time, seconds int, local bool) Quality {
	source := "SSH只读采集"
	if local {
		source = "本机只读采集"
	}
	if a.cfg.Fixture {
		source = "隔离演示数据"
	}
	q := Quality{State: "live", Source: source, ObservedAt: stamp(now), LastSuccess: stamp(now), LastAttempt: stamp(now), ExpectedSeconds: seconds}
	if a.cfg.Fixture {
		q.State = "demo"
	}
	if err != nil {
		q.State = "error"
		q.ObservedAt = previous.ObservedAt
		q.LastSuccess = previous.LastSuccess
		q.Error = "collection_failed"
	}
	return q
}
func (a *App) apply(h *HostView, mode string, b []byte, e error, now time.Time, seconds int) {
	switch mode {
	case "metrics":
		var raw rawMetrics
		var v Metrics
		if e == nil {
			e = json.Unmarshal(b, &raw)
		}
		if e == nil {
			v, e = calculate(raw, a.previous[h.ID], now)
		}
		q := a.quality(h.Metrics.Quality, e, now, seconds, h.Local)
		h.Metrics.Quality = q
		if e != nil {
			h.Metrics.Values = nil
			a.previous[h.ID] = nil
			h.Failures++
			a.successes[h.ID] = 0
			h.Connection = "unknown"
			a.rules.Invalidate(h.ID)
			a.saveError(a.store.gap(h.ID, now))
			if h.Failures >= 3 {
				h.Connection = "unreachable"
				a.store.event(h.ID+":unreachable", h.ID, "critical", "采集端无法连接主机", "连续三次新鲜采集失败；请区分网络、认证和采集模块", now)
			}
			return
		}
		h.Metrics.Values = &v
		a.previous[h.ID] = &rawSample{raw, now}
		a.successes[h.ID]++
		if a.successes[h.ID] >= 2 {
			h.Failures = 0
			a.store.resolve(h.ID+":unreachable", now)
		}
		h.Connection = "connected"
		a.store.resolve(h.ID+":stale", now)
		a.saveError(a.store.metric(h.ID, now, v))
		a.rules.Metrics(*h, now)
	case "services":
		v := ServiceModule{}
		if e == nil {
			e = json.Unmarshal(b, &v)
		}
		if e != nil {
			h.Runtime.Quality = a.quality(h.Runtime.Quality, e, now, seconds, h.Local)
			return
		}
		v.Quality = a.quality(h.Runtime.Quality, nil, now, seconds, h.Local)
		if v.DockerError != "" || v.ComposeError != "" {
			v.Quality.State = "partial"
		}
		h.Runtime = v
		a.saveError(a.store.save("runtime", h.ID, v))
		if v.DockerError == "" {
			for _, c := range v.Containers {
				key := h.ID + ":container:" + c.Name
				a.rules.threshold(key, h.ID, "容器异常："+c.Name, containerFailure(c), !containerFailure(c), time.Duration(seconds)*time.Second, time.Duration(seconds)*time.Second, "warning", now)
			}
		}
		for _, s := range v.Services {
			if s.State == "unknown" || s.State == "not_found" {
				continue
			}
			key := h.ID + ":service:" + s.Name
			a.rules.threshold(key, h.ID, "服务异常："+s.Name, s.State != "active", s.State == "active", time.Duration(seconds)*time.Second, time.Duration(seconds)*time.Second, "warning", now)
		}
	case "inventory":
		var v Inventory
		if e == nil {
			e = json.Unmarshal(b, &v)
		}
		if e != nil {
			h.Inventory.Quality = a.quality(h.Inventory.Quality, e, now, seconds, h.Local)
			a.next[h.ID+":inventory"] = now.Add(10 * time.Minute)
			return
		}
		v.Quality = a.quality(h.Inventory.Quality, nil, now, seconds, h.Local)
		if len(v.Errors) > 0 {
			v.Quality.State = "partial"
		}
		for i := range v.Software {
			v.Software[i].ObservedAt = stamp(now)
		}
		for i := range v.Packages {
			v.Packages[i].ObservedAt = stamp(now)
		}
		v.PackageCount = len(v.Packages)
		current := map[string]bool{}
		for _, s := range v.Software {
			current[strings.ToLower(s.Name)] = true
		}
		for _, s := range h.Inventory.Software {
			if !current[strings.ToLower(s.Name)] {
				v.Software = append(v.Software, s)
			}
		}
		h.Inventory = v
		a.saveError(a.store.save("inventory", h.ID, v))
	}
}
func (a *App) dependencies(hosts []HostView, now time.Time) []Dependency {
	deps := []Dependency{{ID: "model-tunnel", From: "t2", To: "t1", Label: "Claude → SSH → LiteLLM", State: "unknown", Detail: "当前链路尚待采集；历史已存模型调用验收"}, {ID: "proxy-egress", From: "t3", To: "t1", Label: "Privoxy → SOCKS5 → 出口", State: "unknown", Detail: "未配置当前端到端业务探测"}}
	get := func(id string) *HostView {
		for i := range hosts {
			if hosts[i].ID == id {
				return &hosts[i]
			}
		}
		return nil
	}
	t1 := get("t1")
	for i := range deps {
		d := &deps[i]
		h := get(d.From)
		if h == nil || !h.Enabled {
			d.State = "pending"
			d.Detail = "对应主机尚未进入启用阶段"
			continue
		}
		if t1 == nil || t1.Connection != "connected" || t1.Metrics.Quality.State != "live" && t1.Metrics.Quality.State != "demo" {
			d.Detail = "t1状态未知或不可达，依赖可能受影响，需探测确认"
			continue
		}
		q := h.Runtime.Quality.fresh(now)
		if q.State != "live" && q.State != "demo" && q.State != "partial" {
			d.Detail = "链路采集失败或过期，当前状态未知"
			continue
		}
		d.ObservedAt = q.ObservedAt
		if d.ID == "model-tunnel" {
			for _, p := range h.Runtime.Probes {
				if p.Name == "litellm_gateway" {
					d.State = p.State
					d.Detail = "网关存活探测；当前推理业务未验证"
				}
			}
		} else {
			ok := 0
			for _, p := range h.Runtime.Probes {
				if p.State == "listener_only" {
					ok++
				}
			}
			if ok == 2 {
				d.State = "listener_only"
				d.Detail = "本地代理监听可连接；端到端出口仍未验证"
			}
		}
	}
	return deps
}
func (a *App) refresh(now time.Time) {
	hosts := make([]HostView, len(a.hosts))
	copy(hosts, a.hosts)
	for i := range hosts {
		h := &hosts[i]
		h.Inventory.Packages = nil
		h.Metrics.Quality = h.Metrics.Quality.fresh(now)
		h.Runtime.Quality = h.Runtime.Quality.fresh(now)
		if h.Metrics.Quality.State == "stale" {
			h.Connection = "stale"
			h.Metrics.Values = nil
		}
		if !h.Enabled {
			h.Connection = "pending"
		}
	}
	events, e := a.store.events(30, 0)
	if e != nil {
		events = []Event{}
	}
	v := Overview{Version: a.version, Fixture: a.cfg.Fixture, GeneratedAt: stamp(now), Hosts: hosts, Dependencies: a.dependencies(hosts, now), Events: events, Rollout: a.rollout, Resource: a.resource, Degraded: a.guard.Degraded}
	a.cache, _ = json.Marshal(v)
}
func (a *App) fixture(h HostView, mode string, now time.Time) []byte {
	switch mode {
	case "metrics":
		wave := uint64(12 + int(8*math.Sin(float64(now.Unix())/10)))
		ticks := uint64(now.Unix()) * 100
		raw := rawMetrics{CPUTicks: []uint64{ticks * wave, 0, ticks * 4, ticks * 70, 0, 0, 0, 0}, MemoryTotal: h.Inventory.MemoryTotal, MemoryAvailable: h.Inventory.MemoryTotal / 2, DiskTotal: h.Inventory.DiskTotal, DiskFree: h.Inventory.DiskTotal / 2, DiskPercent: 50, Load: []float64{.2, .3, .2}, Uptime: 86400, Cores: h.Inventory.Cores, RX: uint64(now.Unix()) * 20000, TX: uint64(now.Unix()) * 12000, BootID: "demo"}
		b, _ := json.Marshal(raw)
		return b
	case "services":
		v := h.Runtime
		v.DockerError = ""
		v.ComposeError = ""
		v.Containers = append([]Container(nil), v.Containers...)
		for i := range v.Containers {
			c := &v.Containers[i]
			c.State = "running"
			c.Health = "not_configured"
			if c.ExpectedExit {
				c.State = "exited"
			}
		}
		v.Probes = []Probe{{Name: "litellm_gateway", State: "gateway_reachable", Status: 200}}
		b, _ := json.Marshal(v)
		return b
	default:
		v := h.Inventory
		b, _ := json.Marshal(v)
		return b
	}
}
func (a *App) SeedFixtureHistory() error {
	if !a.cfg.Fixture {
		return errors.New("fixture only")
	}
	now := time.Now()
	for _, h := range a.hosts {
		for i := 119; i >= 0; i-- {
			at := now.Add(-time.Duration(i) * 30 * time.Second)
			v := Metrics{CPU: number(25 + 12*math.Sin(float64(i)/9)), MemoryPercent: 50, MemoryTotal: h.Inventory.MemoryTotal, MemoryAvailable: h.Inventory.MemoryTotal / 2, DiskTotal: h.Inventory.DiskTotal, DiskPercent: 50, RX: number(20000), TX: number(12000), Cores: h.Inventory.Cores}
			if e := a.store.metric(h.ID, at, v); e != nil {
				return e
			}
		}
	}
	return a.store.info("demo", "t1", "演示事件：网关探测恢复", "此事件仅存在于隔离演示数据库", now)
}
