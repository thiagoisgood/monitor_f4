package fleet

import "time"

type ruleState struct {
	Since    time.Time
	Recovery time.Time
	Active   bool
}
type Rules struct {
	items map[string]*ruleState
	store *Store
}

func NewRules(s *Store) *Rules { return &Rules{map[string]*ruleState{}, s} }
func (r *Rules) threshold(key, host, title string, bad, recovered bool, wait, recovery time.Duration, severity string, now time.Time) {
	v := r.items[key]
	if v == nil {
		v = &ruleState{}
		var count int
		r.store.db.QueryRow("SELECT count(*) FROM events WHERE key=? AND state='active'", key).Scan(&count)
		v.Active = count > 0
		r.items[key] = v
	}
	if bad {
		v.Recovery = time.Time{}
		if v.Since.IsZero() {
			v.Since = now
		}
		if now.Sub(v.Since) >= wait {
			r.store.event(key, host, severity, title, "连续新鲜采样触发；恢复需满足回落条件", now)
			v.Active = true
		}
	} else {
		v.Since = time.Time{}
		if recovered && v.Active {
			if v.Recovery.IsZero() {
				v.Recovery = now
			}
			if now.Sub(v.Recovery) >= recovery {
				r.store.resolve(key, now)
				v.Active = false
				v.Recovery = time.Time{}
			}
		} else {
			v.Recovery = time.Time{}
		}
	}
}
func (r *Rules) Metrics(h HostView, now time.Time) {
	v := h.Metrics.Values
	if v == nil {
		r.Invalidate(h.ID)
		return
	}
	if v.CPU != nil {
		r.threshold(h.ID+":cpu", h.ID, "CPU 持续偏高", *v.CPU > 85, *v.CPU < 75, 5*time.Minute, 2*time.Minute, "warning", now)
	}
	r.threshold(h.ID+":memory", h.ID, "内存持续偏高", v.MemoryPercent > 85, v.MemoryPercent < 75, 3*time.Minute, 2*time.Minute, "warning", now)
	r.threshold(h.ID+":disk", h.ID, "根盘空间预警", v.DiskPercent >= 80, v.DiskPercent < 75, 0, 30*time.Second, "warning", now)
	r.threshold(h.ID+":disk_critical", h.ID, "根盘空间严重不足", v.DiskPercent >= 90, v.DiskPercent < 85, 0, 30*time.Second, "critical", now)
}
func (r *Rules) Invalidate(host string) {
	for key, v := range r.items {
		if len(key) > len(host) && key[:len(host)+1] == host+":" {
			v.Since = time.Time{}
			v.Recovery = time.Time{}
		}
	}
}
func containerFailure(c Container) bool {
	return (!c.ExpectedExit && c.State != "running") || c.Health == "unhealthy"
}
