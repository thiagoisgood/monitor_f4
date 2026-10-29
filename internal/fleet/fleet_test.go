package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func validRaw() rawMetrics {
	return rawMetrics{CPUTicks: []uint64{100, 0, 50, 850, 0, 0, 0, 0}, MemoryTotal: 1024, MemoryAvailable: 512, DiskTotal: 1024, DiskFree: 512, DiskPercent: 50, Load: []float64{0, 0, 0}, Cores: 2, BootID: "one", RX: 100, TX: 100}
}
func TestMetricDeltasAndResets(t *testing.T) {
	now := time.Now()
	raw := validRaw()
	first, e := calculate(raw, nil, now)
	if e != nil || first.CPU != nil || first.RX != nil {
		t.Fatal("first sample must remain unknown", first, e)
	}
	previous := rawSample{raw, now}
	previous.CPUTicks = append([]uint64(nil), raw.CPUTicks...)
	raw.CPUTicks[0] += 20
	raw.CPUTicks[3] += 80
	raw.RX += 300
	raw.TX += 600
	v, e := calculate(raw, &previous, now.Add(30*time.Second))
	if e != nil || v.CPU == nil || *v.CPU != 20 || *v.RX != 10 || *v.TX != 20 {
		t.Fatalf("incorrect deltas: %+v %v", v, e)
	}
	raw.BootID = "two"
	v, _ = calculate(raw, &previous, now.Add(60*time.Second))
	if v.CPU != nil || v.RX != nil {
		t.Fatal("reboot must create a gap")
	}
	raw.BootID = "one"
	raw.RX = 1
	raw.CPUTicks[0] = 1
	v, _ = calculate(raw, &previous, now.Add(time.Minute))
	if v.CPU != nil || v.RX != nil {
		t.Fatal("counter rollback must create a gap")
	}
	raw.MemoryTotal = 0
	if _, e = calculate(raw, nil, now); e == nil {
		t.Fatal("invalid metrics accepted")
	}
}
func TestGuardDegradeStopAndBudget(t *testing.T) {
	now := time.Now()
	g := Guard{}
	v := Resource{Available: 340 * MiB, Memory: 32 * MiB, HealthStatus: 200}
	for i := 0; i < 3; i++ {
		if stop, _ := g.Check(v, now.Add(time.Duration(i)*10*time.Second), true); stop {
			t.Fatal("premature stop")
		}
	}
	if !g.Degraded {
		t.Fatal("did not degrade")
	}
	v.Available = 240 * MiB
	for i := 0; i < 2; i++ {
		if stop, _ := g.Check(v, now.Add(time.Duration(i+3)*10*time.Second), true); stop {
			t.Fatal("critical memory requires three samples")
		}
	}
	if stop, reason := g.Check(v, now.Add(time.Minute), true); !stop || reason != "host_available_memory_below_250MiB" {
		t.Fatal(stop, reason)
	}
	g = Guard{}
	v = Resource{Available: 600 * MiB, Memory: 100 * MiB, HealthStatus: 200}
	for i := 0; i < 3; i++ {
		g.Check(v, now.Add(time.Duration(i)*10*time.Second), true)
	}
	if stop, _ := g.Check(v, now.Add(201*time.Second), true); !stop {
		t.Fatal("persistent excess did not stop")
	}
	g = Guard{}
	v.OOM = 1
	if stop, _ := g.Check(v, now, true); !stop {
		t.Fatal("OOM ignored")
	}
}
func newTestApp(t *testing.T) *App {
	t.Helper()
	a, e := NewApp(Config{Fixture: true, DataDir: t.TempDir(), Bind: "127.0.0.1:18090", StageSeconds: 900}, "test", nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(a.Close)
	return a
}
func TestFailureProvenanceAndRecovery(t *testing.T) {
	a := newTestApp(t)
	h := &a.hosts[0]
	now := time.Now()
	raw := validRaw()
	b, _ := json.Marshal(raw)
	a.apply(h, "metrics", b, nil, now, 30)
	for i := 1; i <= 3; i++ {
		a.apply(h, "metrics", nil, context.DeadlineExceeded, now.Add(time.Duration(i)*30*time.Second), 30)
	}
	if h.Connection != "unreachable" || h.Metrics.Values != nil || h.Metrics.Quality.LastSuccess == "" {
		t.Fatalf("bad failure state %+v", h)
	}
	a.apply(h, "metrics", b, nil, now.Add(120*time.Second), 30)
	if h.Failures == 0 {
		t.Fatal("recovered too soon")
	}
	a.apply(h, "metrics", b, nil, now.Add(150*time.Second), 30)
	if h.Failures != 0 {
		t.Fatal("two-sample recovery failed")
	}
	events, _ := a.store.events(10, 0)
	if len(events) != 1 || events[0].State != "resolved" {
		t.Fatal(events)
	}
	a.refresh(now.Add(300 * time.Second))
	var overview Overview
	json.Unmarshal(a.cache, &overview)
	if overview.Hosts[0].Metrics.Values != nil || overview.Hosts[0].Connection != "stale" {
		t.Fatal("stale values shown as live")
	}
}
func TestRulesExpectedExitAndDedup(t *testing.T) {
	a := newTestApp(t)
	now := time.Now()
	r := a.rules
	r.threshold("rule", "t1", "test", true, false, 0, 30*time.Second, "warning", now)
	r.threshold("rule", "t1", "test", true, false, 0, 30*time.Second, "warning", now.Add(time.Minute))
	events, _ := a.store.events(10, 0)
	if len(events) != 1 {
		t.Fatal("duplicate events", events)
	}
	r.threshold("rule", "t1", "test", false, true, 0, 30*time.Second, "warning", now.Add(2*time.Minute))
	r.threshold("rule", "t1", "test", false, true, 0, 30*time.Second, "warning", now.Add(3*time.Minute))
	events, _ = a.store.events(10, 0)
	if events[0].State != "resolved" {
		t.Fatal("not resolved")
	}
	if containerFailure(Container{State: "exited", ExpectedExit: true}) {
		t.Fatal("expected init exit misclassified")
	}
	if !containerFailure(Container{State: "exited"}) {
		t.Fatal("unexpected stopped service missed")
	}
	if containerFailure(Container{State: "running", Health: "not_configured"}) {
		t.Fatal("missing healthcheck isn't failure")
	}
}
func TestHistoryLimitGapPeakAndRetention(t *testing.T) {
	a := newTestApp(t)
	now := time.Now().Truncate(time.Second)
	for i := 0; i < 1200; i++ {
		at := now.Add(time.Duration(i-1200) * time.Second)
		if i == 5 {
			a.store.gap("t1", at)
		} else {
			a.store.metric("t1", at, Metrics{CPU: number(float64(i % 100))})
		}
	}
	points, e := a.store.history("t1", now.Add(-time.Hour), now)
	if e != nil || len(points) != 600 {
		t.Fatal(len(points), e)
	}
	gap := false
	peak := false
	for _, p := range points {
		gap = gap || p.Values == nil
		peak = peak || (p.Values != nil && p.Values.CPU != nil && *p.Values.CPU == 99)
	}
	if !gap || !peak {
		t.Fatal("gap or peak lost")
	}
	a.store.metric("t2", now.Add(-25*time.Hour), Metrics{})
	a.store.cleanup(now)
	var count int
	a.store.db.QueryRow("SELECT count(*) FROM metrics WHERE host='t2'").Scan(&count)
	if count != 0 {
		t.Fatal("retention failed")
	}
}
func TestAPIReadOnlyBoundsHostAndNoCollection(t *testing.T) {
	a := newTestApp(t)
	assets := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("fleet")}}
	handler := a.Handler(fs.FS(assets))
	tests := []struct {
		method, path, host string
		code               int
	}{{"GET", "/api/v1/overview", "127.0.0.1:18090", 200}, {"POST", "/api/v1/overview", "localhost", 405}, {"GET", "/api/v1/overview", "attacker.invalid", 403}, {"GET", "/api/v1/events?limit=201", "localhost", 400}, {"GET", "/api/v1/events?offset=-1", "localhost", 400}, {"GET", "/api/v1/hosts/unknown/metrics", "localhost", 404}, {"GET", "/api/v1/hosts/t1/inventory?packages_page=0", "localhost", 400}, {"GET", "/api/v1/hosts/t1/metrics?from=2000-01-01T00:00:00Z", "localhost", 400}}
	for _, c := range tests {
		request := httptest.NewRequest(c.method, c.path, nil)
		request.Host = c.host
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, request)
		if w.Code != c.code {
			t.Errorf("%s => %d wanted %d", c.path, w.Code, c.code)
		}
	}
	if len(a.previous) != 0 {
		t.Fatal("API access triggered collection")
	}
	req := httptest.NewRequest("GET", "/api/v1/overview", nil)
	req.Host = "localhost"
	req.Header.Set("Origin", "https://attacker.invalid")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatal("foreign origin accepted")
	}
}
func TestPauseMarkerBlocksRestartAndListenerProtection(t *testing.T) {
	dir := t.TempDir()
	pauseMarker(dir, "test")
	if _, e := NewApp(Config{Fixture: true, DataDir: dir}, "test", nil); e == nil {
		t.Fatal("pause marker ignored")
	}
	p := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(p, []byte(`{"bind":"0.0.0.0:18090","data_dir":"/tmp/test"}`), 0600)
	if _, e := ReadConfig(p); e == nil {
		t.Fatal("public listener accepted")
	}
	os.WriteFile(p, []byte(`{"bind":"127.0.0.1:18090","stage_seconds":60}`), 0600)
	if _, e := ReadConfig(p); e == nil {
		t.Fatal("shortened production stage accepted")
	}
}
func TestRolloutRequiresHealthyFullWindow(t *testing.T) {
	a := newTestApp(t)
	a.cfg.Fixture = false
	a.rollout.Enabled = 1
	for i := range a.hosts {
		a.hosts[i].Enabled = i == 0
	}
	a.rollout.State = "canary"
	now := time.Now()
	a.rollout.PhaseStarted = now.Add(-901 * time.Second)
	a.resource = Resource{Memory: 32 * MiB, CPU15m: .2, HealthStatus: 200}
	a.hosts[0].Metrics.Quality = Quality{State: "live", LastSuccess: stamp(now), ExpectedSeconds: 30}
	a.hosts[0].Runtime.Quality = Quality{State: "live", LastSuccess: stamp(now), ExpectedSeconds: 60}
	a.hosts[0].Runtime.DockerError = ""
	a.advance(now)
	if a.rollout.Enabled != 2 || !a.hosts[1].Enabled {
		t.Fatal("healthy stage did not advance")
	}
	a.rollout.PhaseStarted = now.Add(-901 * time.Second)
	a.advance(now)
	if a.rollout.Enabled != 2 {
		t.Fatal("advanced while next host wasn't sampled")
	}
	a.guard.Degraded = true
	a.rollout.PhaseStarted = now.Add(-901 * time.Second)
	a.advance(now)
	if a.rollout.Enabled != 2 {
		t.Fatal("advanced under pressure")
	}
}
func TestEmbeddedCollectorHasFixedInputsAndNoMutations(t *testing.T) {
	for _, forbidden := range []string{"docker inspect", "restart(", "apt install", "pip install", "open(path, 'w')"} {
		if strings.Contains(collectorScript, forbidden) {
			t.Fatal(forbidden)
		}
	}
	if !strings.Contains(collectorScript, "sudo', '-n', 'docker") {
		t.Fatal("t2 permission strategy missing")
	}
}

func TestStageOneCapSurvivesRestartAndNeverCollectsOtherHosts(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{DataDir: dir, StageSeconds: 900, MaxHosts: 1}
	a, e := NewApp(cfg, "stage1", nil)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	a.rollout.PhaseStarted = now.Add(-16 * time.Minute)
	a.resource = Resource{Memory: 32 * MiB, CPU15m: .2, HealthStatus: 200}
	a.hosts[0].Metrics.Quality = Quality{State: "live", LastSuccess: stamp(now), ExpectedSeconds: 30}
	a.hosts[0].Runtime.Quality = Quality{State: "live", LastSuccess: stamp(now), ExpectedSeconds: 60}
	a.hosts[0].Runtime.DockerError = ""
	a.advance(now)
	if a.rollout.Enabled != 1 || a.rollout.State != "stage_complete" || a.rollout.ObservationDue != nil {
		t.Fatalf("stage cap exceeded or 24h observation started: %+v", a.rollout)
	}
	// A saved four-host rollout must also obey an explicitly reduced deployment scope.
	a.store.save("state", "rollout", Rollout{Enabled: 4, AllEnabledAt: &now, ObservationDue: &now})
	a.Close()
	a, e = NewApp(cfg, "stage1", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if a.rollout.Enabled != 1 || a.rollout.ObservationDue != nil || a.rollout.AllEnabledAt != nil {
		t.Fatal("saved rollout bypassed stage cap", a.rollout)
	}
	for i, h := range a.hosts {
		if h.Enabled != (i == 0) {
			t.Fatal("unexpected enabled host", h.ID)
		}
	}
}

func TestConfigRejectsInvalidHostCaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	for _, cap := range []int{-1, 0, 5} {
		os.WriteFile(path, []byte(fmt.Sprintf(`{"max_hosts":%d}`, cap)), 0600)
		if _, e := ReadConfig(path); e == nil {
			t.Fatalf("accepted cap %d", cap)
		}
	}
}

func TestLowDiskSuspendsAndRequiresManualRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Fixture: true, DataDir: dir, StageSeconds: 900}
	a, e := NewApp(cfg, "isolated-low-disk", nil)
	if e != nil {
		t.Fatal(e)
	}
	a.sampleResource = func(at time.Time) Resource {
		return Resource{At: at, Available: 600 * MiB, DiskFree: 1024 * MiB, Memory: 32 * MiB, HealthStatus: 200}
	}
	a.checkGuard(time.Now())
	if !a.hasStop() || a.rollout.State != "suspended" || a.rollout.Reason != "host_disk_free_below_2GiB" {
		t.Fatal("low disk didn't stop monitor", a.rollout)
	}
	marker := filepath.Join(dir, "PAUSED.json")
	if b, e := os.ReadFile(marker); e != nil || !strings.Contains(string(b), "host_disk_free_below_2GiB") {
		t.Fatal("missing protection evidence", e)
	}
	a.Close()
	if app, e := NewApp(cfg, "blocked", nil); e == nil {
		app.Close()
		t.Fatal("restarted without manual recovery")
	}
	if e = os.Remove(marker); e != nil {
		t.Fatal(e)
	}
	cfg.Fixture = false
	a, e = NewApp(cfg, "manually-resumed", nil)
	if e != nil {
		t.Fatal(e)
	}
	if a.rollout.State != "canary" {
		t.Fatal("manual recovery must restart the observation window", a.rollout)
	}
	a.Close()
}

func TestDockerPermissionFailureDoesNotEraseHostMetrics(t *testing.T) {
	a := newTestApp(t)
	now := time.Now()
	h := &a.hosts[0]
	b, _ := json.Marshal(validRaw())
	a.apply(h, "metrics", b, nil, now, 30)
	b, _ = json.Marshal(ServiceModule{DockerError: "permission_denied", Containers: []Container{}, Services: []Service{}})
	a.apply(h, "services", b, nil, now, 60)
	if h.Connection != "connected" || h.Metrics.Values == nil || h.Runtime.Quality.State != "partial" || h.Runtime.DockerError != "permission_denied" {
		t.Fatal("permission failure mislabeled as host failure", h)
	}
	b, _ = json.Marshal(ServiceModule{Containers: []Container{{Name: "pg", State: "running", Health: "not_configured"}}, Services: []Service{}})
	a.apply(h, "services", b, nil, now.Add(time.Minute), 60)
	if h.Runtime.DockerError != "" || h.Runtime.Quality.State != "demo" {
		t.Fatal("module didn't recover", h.Runtime)
	}
}

func TestExtraDiskPropagatesWithoutReplacingRoot(t *testing.T) {
	raw := validRaw()
	raw.ExtraDisk = &ExtraDisk{Mount: "/test", State: "live", Total: 4096, Free: 2048, Percent: 50}
	got, err := calculate(raw, nil, time.Now())
	if err != nil || got.ExtraDisk == nil || got.ExtraDisk.Mount != "/test" || got.ExtraDisk.Free != 2048 || got.DiskTotal != raw.DiskTotal {
		t.Fatalf("extra disk lost or root disk changed: %+v, %v", got, err)
	}
	raw.ExtraDisk = &ExtraDisk{Mount: "/test", State: "unavailable"}
	got, err = calculate(raw, nil, time.Now())
	if err != nil || got.ExtraDisk == nil || got.ExtraDisk.State != "unavailable" {
		t.Fatalf("missing mount lost: %+v, %v", got, err)
	}
	zero, err := json.Marshal(ExtraDisk{Mount: "/test", State: "live", Total: 4096, Percent: 0})
	if err != nil || !strings.Contains(string(zero), `"percent":0`) {
		t.Fatalf("zero utilization must remain visible: %s, %v", zero, err)
	}
}
