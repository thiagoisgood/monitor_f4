package fleet

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
)

func sendJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func invalid(w http.ResponseWriter, why string) { sendJSON(w, 400, map[string]string{"error": why}) }
func (a *App) Handler(assets fs.FS) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/overview", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/overview" {
			http.NotFound(w, r)
			return
		}
		a.mu.RLock()
		cached := append([]byte(nil), a.cache...)
		a.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(cached)
	})
	mux.HandleFunc("/api/v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		sendJSON(w, 200, map[string]any{"status": "ok", "version": a.version, "fixture": a.cfg.Fixture})
	})
	mux.HandleFunc("/api/v1/events", func(w http.ResponseWriter, r *http.Request) {
		limit, offset := 50, 0
		var e error
		if s := r.URL.Query().Get("limit"); s != "" {
			limit, e = strconv.Atoi(s)
			if e != nil || limit < 1 || limit > 200 {
				invalid(w, "limit must be 1..200")
				return
			}
		}
		if s := r.URL.Query().Get("offset"); s != "" {
			offset, e = strconv.Atoi(s)
			if e != nil || offset < 0 || offset > 100000 {
				invalid(w, "invalid offset")
				return
			}
		}
		v, e := a.store.events(limit, offset)
		if e != nil {
			sendJSON(w, 503, map[string]string{"error": "storage_unavailable"})
			return
		}
		sendJSON(w, 200, map[string]any{"events": v, "limit": limit, "offset": offset})
	})
	mux.HandleFunc("/api/v1/observation", func(w http.ResponseWriter, r *http.Request) {
		v, e := a.store.observation()
		if e != nil {
			sendJSON(w, 503, map[string]string{"error": "observation_unavailable"})
			return
		}
		a.mu.RLock()
		v["rollout"] = a.rollout
		v["fixture"] = a.cfg.Fixture
		a.mu.RUnlock()
		sendJSON(w, 200, v)
	})
	mux.HandleFunc("/api/v1/hosts/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/hosts/"), "/")
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}
		id := parts[0]
		if _, ok := hostConfig(id); !ok {
			http.NotFound(w, r)
			return
		}
		if parts[1] == "inventory" {
			page := 0
			if s := r.URL.Query().Get("packages_page"); s != "" {
				n, e := strconv.Atoi(s)
				if e != nil || n < 1 || n > 50 {
					invalid(w, "packages_page must be 1..50")
					return
				}
				page = n
			}
			a.mu.RLock()
			var v Inventory
			for _, h := range a.hosts {
				if h.ID == id {
					v = h.Inventory
					break
				}
			}
			a.mu.RUnlock()
			v.PackageCount = len(v.Packages)
			if page == 0 {
				v.Packages = nil
			} else {
				start := (page - 1) * 100
				if start >= len(v.Packages) {
					v.Packages = []Software{}
				} else {
					end := start + 100
					if end > len(v.Packages) {
						end = len(v.Packages)
					}
					v.Packages = v.Packages[start:end]
				}
				v.PackagePage = page
			}
			sendJSON(w, 200, v)
			return
		}
		if parts[1] == "metrics" {
			now := time.Now()
			from, to := now.Add(-time.Hour), now
			var e error
			if s := r.URL.Query().Get("from"); s != "" {
				from, e = time.Parse(time.RFC3339, s)
				if e != nil {
					invalid(w, "invalid from")
					return
				}
			}
			if s := r.URL.Query().Get("to"); s != "" {
				to, e = time.Parse(time.RFC3339, s)
				if e != nil {
					invalid(w, "invalid to")
					return
				}
			}
			if !to.After(from) || to.Sub(from) > 24*time.Hour || from.Before(now.Add(-24*time.Hour-time.Minute)) || to.After(now.Add(time.Minute)) {
				invalid(w, "range must be within the last 24 hours")
				return
			}
			v, e := a.store.history(id, from, to)
			if e != nil {
				sendJSON(w, 503, map[string]string{"error": "history_unavailable"})
				return
			}
			sendJSON(w, 200, map[string]any{"host": id, "points": v, "max_points": 600, "fixture": a.cfg.Fixture, "downsample": "bucket_cpu_peak_preserving_gaps"})
			return
		}
		http.NotFound(w, r)
	})
	fileServer := http.FileServer(http.FS(assets))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/assets/") {
			w.Header().Set("Cache-Control", "public,max-age=31536000,immutable")
		}
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && (strings.HasSuffix(r.URL.Path, ".js") || strings.HasSuffix(r.URL.Path, ".css")) {
			if b, e := fs.ReadFile(assets, strings.TrimPrefix(r.URL.Path, "/")+".gz"); e == nil {
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Vary", "Accept-Encoding")
				w.Header().Set("Content-Type", mime.TypeByExtension(path.Ext(r.URL.Path)))
				w.Header().Set("Content-Length", strconv.Itoa(len(b)))
				if r.Method != "HEAD" {
					w.Write(b)
				}
				return
			}
		}
		fileServer.ServeHTTP(w, r)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, e := net.SplitHostPort(host); e == nil {
			host = h
		}
		if host != "localhost" && host != "127.0.0.1" && host != "::1" {
			http.Error(w, "invalid host", 403)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
		if strings.HasPrefix(r.URL.Path, "/api/") && r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			sendJSON(w, 405, map[string]string{"error": "read_only"})
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "read only", 405)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host {
			http.Error(w, "invalid origin", 403)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func (s *Store) observation() (map[string]any, error) {
	rows, e := s.db.Query("SELECT body FROM resources ORDER BY ts")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	n := 0
	peak := uint64(0)
	minAvail := ^uint64(0)
	cpu := 0.0
	peakCPU15 := 0.0
	badHealth := 0
	oom := uint64(0)
	var first, last time.Time
	for rows.Next() {
		var b string
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		var v Resource
		if e = json.Unmarshal([]byte(b), &v); e != nil {
			return nil, e
		}
		if n == 0 {
			first = v.At
		}
		last = v.At
		n++
		if v.Memory > peak {
			peak = v.Memory
		}
		if v.Available < minAvail {
			minAvail = v.Available
		}
		cpu += v.CPUPercent
		if v.CPU15m > peakCPU15 {
			peakCPU15 = v.CPU15m
		}
		if v.HealthStatus != 200 {
			badHealth++
		}
		if v.OOM > oom {
			oom = v.OOM
		}
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if n == 0 {
		minAvail = 0
	} else {
		cpu /= float64(n)
	}
	return map[string]any{"samples": n, "first_at": first, "last_at": last, "duration_seconds": last.Sub(first).Seconds(), "peak_memory_bytes": peak, "min_available_bytes": minAvail, "cpu_average_percent_one_core": cpu, "peak_cpu_15m_percent_one_core": peakCPU15, "bad_gateway_health_samples": badHealth, "oom_kills": oom, "business_performance": "not_verified_without_comparable_traffic", "summary": fmt.Sprintf("%d resource observations", n)}, nil
}
