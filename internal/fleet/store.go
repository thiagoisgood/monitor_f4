package fleet

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "modernc.org/sqlite"
	"path/filepath"
	"time"
)

type Store struct{ db *sql.DB }

func OpenStore(dir string) (*Store, error) {
	db, e := sql.Open("sqlite", filepath.Join(dir, "fleet.db"))
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`PRAGMA journal_mode=DELETE;PRAGMA synchronous=NORMAL;PRAGMA busy_timeout=2000;PRAGMA cache_size=-8192;PRAGMA max_page_count=131072;
 CREATE TABLE IF NOT EXISTS metrics(host TEXT NOT NULL,ts INTEGER NOT NULL,body TEXT NOT NULL,PRIMARY KEY(host,ts));
 CREATE TABLE IF NOT EXISTS inventory(host TEXT PRIMARY KEY,body TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS runtime(host TEXT PRIMARY KEY,body TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,body TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS resources(ts INTEGER PRIMARY KEY,body TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT NOT NULL,host TEXT NOT NULL,severity TEXT NOT NULL,title TEXT NOT NULL,detail TEXT NOT NULL,state TEXT NOT NULL,first_at TEXT NOT NULL,last_at TEXT NOT NULL,resolved_at TEXT NOT NULL DEFAULT '');
 CREATE UNIQUE INDEX IF NOT EXISTS events_active ON events(key) WHERE state='active';`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) save(table, key string, v any) error {
	if table != "inventory" && table != "runtime" && table != "state" {
		return fmt.Errorf("invalid table")
	}
	column := "host"
	if table == "state" {
		column = "key"
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = s.db.Exec("INSERT INTO "+table+"("+column+",body) VALUES(?,?) ON CONFLICT("+column+") DO UPDATE SET body=excluded.body", key, string(b))
	return e
}
func (s *Store) load(table, key string, v any) error {
	if table != "inventory" && table != "runtime" && table != "state" {
		return fmt.Errorf("invalid table")
	}
	column := "host"
	if table == "state" {
		column = "key"
	}
	var b string
	if e := s.db.QueryRow("SELECT body FROM "+table+" WHERE "+column+"=?", key).Scan(&b); e != nil {
		return e
	}
	return json.Unmarshal([]byte(b), v)
}
func (s *Store) metric(host string, t time.Time, v Metrics) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = s.db.Exec("INSERT OR REPLACE INTO metrics VALUES(?,?,?)", host, t.Unix(), string(b))
	return e
}

type Point struct {
	At     string   `json:"at"`
	Values *Metrics `json:"values"`
}

func (s *Store) history(host string, from, to time.Time) ([]Point, error) {
	rows, e := s.db.Query("SELECT ts,body FROM metrics WHERE host=? AND ts>=? AND ts<=? ORDER BY ts", host, from.Unix(), to.Unix())
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	all := []Point{}
	for rows.Next() {
		var ts int64
		var b string
		if e = rows.Scan(&ts, &b); e != nil {
			return nil, e
		}
		var v *Metrics
		if e = json.Unmarshal([]byte(b), &v); e != nil {
			return nil, e
		}
		all = append(all, Point{stamp(time.Unix(ts, 0)), v})
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if len(all) <= 600 {
		return all, nil
	}
	out := make([]Point, 0, 600)
	for i := 0; i < 600; i++ {
		start, end := i*len(all)/600, (i+1)*len(all)/600
		chosen := all[end-1]
		peak := -1.0
		for _, p := range all[start:end] {
			if p.Values == nil {
				chosen = p
				break
			}
			if p.Values.CPU != nil && *p.Values.CPU > peak {
				peak = *p.Values.CPU
				chosen = p
			}
		}
		out = append(out, chosen)
	}
	return out, nil
}
func (s *Store) gap(host string, t time.Time) error {
	_, e := s.db.Exec("INSERT OR REPLACE INTO metrics VALUES(?,?,?)", host, t.Unix(), "null")
	return e
}
func (s *Store) resource(v Resource) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = s.db.Exec("INSERT OR REPLACE INTO resources VALUES(?,?)", v.At.Unix(), string(b))
	return e
}
func (s *Store) event(key, host, severity, title, detail string, now time.Time) error {
	_, e := s.db.Exec(`INSERT INTO events(key,host,severity,title,detail,state,first_at,last_at) VALUES(?,?,?,?,?,'active',?,?) ON CONFLICT(key) WHERE state='active' DO UPDATE SET last_at=excluded.last_at,detail=excluded.detail,severity=excluded.severity`, key, host, severity, title, detail, stamp(now), stamp(now))
	return e
}
func (s *Store) info(key, host, title, detail string, now time.Time) error {
	_, e := s.db.Exec("INSERT INTO events(key,host,severity,title,detail,state,first_at,last_at) VALUES(?,?,'info',?,?,'info',?,?)", key, host, title, detail, stamp(now), stamp(now))
	return e
}
func (s *Store) resolve(key string, now time.Time) error {
	_, e := s.db.Exec("UPDATE events SET state='resolved',resolved_at=?,last_at=? WHERE key=? AND state='active'", stamp(now), stamp(now), key)
	return e
}
func (s *Store) events(limit, offset int) ([]Event, error) {
	rows, e := s.db.Query("SELECT id,key,host,severity,title,detail,state,first_at,last_at,resolved_at FROM events ORDER BY id DESC LIMIT ? OFFSET ?", limit, offset)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var v Event
		if e = rows.Scan(&v.ID, &v.Key, &v.Host, &v.Severity, &v.Title, &v.Detail, &v.State, &v.FirstAt, &v.LastAt, &v.ResolvedAt); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) cleanup(now time.Time) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, q := range []struct {
		q string
		t int64
	}{{"DELETE FROM metrics WHERE rowid IN (SELECT rowid FROM metrics WHERE ts<? LIMIT 1000)", now.Add(-24 * time.Hour).Unix()}, {"DELETE FROM resources WHERE ts IN (SELECT ts FROM resources WHERE ts<? LIMIT 1000)", now.Add(-48 * time.Hour).Unix()}} {
		if _, e = tx.Exec(q.q, q.t); e != nil {
			return e
		}
	}
	if _, e = tx.Exec("DELETE FROM events WHERE id IN (SELECT id FROM events WHERE last_at<? AND state!='active' LIMIT 1000)", stamp(now.Add(-7*24*time.Hour))); e != nil {
		return e
	}
	return tx.Commit()
}
