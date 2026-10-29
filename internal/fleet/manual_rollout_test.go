package fleet

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManualRolloutEnablesHostsWithoutStartingObservation(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), StageSeconds: 900, MaxHosts: 4, RolloutMode: "manual"}
	a, err := NewApp(cfg, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.rollout.Enabled != 4 || a.rollout.State != "awaiting_observation" {
		t.Fatal("manual host activation was not applied")
	}
	for _, h := range a.hosts {
		if !h.Enabled {
			t.Fatal("manual host left disabled", h.ID)
		}
	}
	a.advance(time.Now().Add(48 * time.Hour))
	if a.rollout.State != "awaiting_observation" || a.rollout.AllEnabledAt != nil || a.rollout.ObservationDue != nil {
		t.Fatal("manual activation started an observation timer")
	}
	a.Close()
	a, err = NewApp(cfg, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.rollout.Enabled != 4 || a.rollout.ObservationDue != nil {
		t.Fatal("manual state did not survive restart")
	}
	a.Close()
	cfg.RolloutMode = "staged"
	a, err = NewApp(cfg, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.rollout.State != "observing_24h" || a.rollout.AllEnabledAt == nil || a.rollout.ObservationDue == nil {
		t.Fatal("explicit observation start did not create a fresh window")
	}
	if a.rollout.ObservationDue.Sub(*a.rollout.AllEnabledAt) != 24*time.Hour {
		t.Fatal("observation window is not 24 hours")
	}
}

func TestManualRolloutRetainsResourceProtection(t *testing.T) {
	a := newTestApp(t)
	a.cfg.RolloutMode = "manual"
	a.cfg.Fixture = false
	a.sampleResource = func(now time.Time) Resource {
		return Resource{At: now, Memory: 32 * MiB, Available: 800 * MiB,
			DiskFree: 10 * 1024 * MiB, HealthStatus: 200, OOM: 1}
	}
	a.checkGuard(time.Now())
	if a.rollout.State != "suspended" || !a.hasStop() {
		t.Fatal("manual mode bypassed OOM protection")
	}
}

func TestRolloutModeRejectsTypos(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	for _, mode := range []string{"manual", "staged", "manul"} {
		if err := os.WriteFile(p, []byte(`{"rollout_mode":"`+mode+`"}`), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := ReadConfig(p)
		if (err != nil) != (mode == "manul") {
			t.Fatalf("unexpected validation result for %q: %v", mode, err)
		}
	}
}
