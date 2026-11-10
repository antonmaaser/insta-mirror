package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	DB     *sql.DB
	readTx *sql.Tx
}

type Record struct {
	Table          string          `json:"kind"`
	RowID          int64           `json:"record_id"`
	Account        string          `json:"account_id"`
	Identity       string          `json:"source_identity"`
	Thread         string          `json:"acquired_thread_id,omitempty"`
	Origin         string          `json:"origin"`
	Observed       int64           `json:"observed_at_ms"`
	LastObserved   int64           `json:"last_observed_at_ms"`
	Hash           string          `json:"source_hash"`
	Raw            json.RawMessage `json:"raw,omitempty"`
	Version        int64           `json:"-"`
	ContentHash    string          `json:"normalized_content_hash"`
	Representation string          `json:"representation"`
	Acquisitions   []Acquisition   `json:"acquisitions,omitempty"`
}

// Acquisition identifies how a version was observed, independently of its content.
type Acquisition struct {
	Origin       string `json:"origin"`
	Thread       string `json:"acquired_thread_id,omitempty"`
	Hash         string `json:"source_hash"`
	Observed     int64  `json:"observed_at_ms"`
	LastObserved int64  `json:"last_observed_at_ms"`
}

type State struct {
	PageHash    string   `json:"page_hash,omitempty"`
	Cursor      string   `json:"cursor,omitempty"`
	Complete    bool     `json:"complete"`
	SeqID       int64    `json:"seq_id,omitempty"`
	Timestamp   int64    `json:"timestamp_ms,omitempty"`
	LastSuccess int64    `json:"last_success_ms,omitempty"`
	Error       string   `json:"error,omitempty"`
	Gaps        []string `json:"gaps,omitempty"`
}

const schemaVersion = 3

func Open(path string) (*Store, error) {
	// Reject incompatible files before permissions, journal mode, or schema change.
	if _, err := os.Stat(path); err == nil {
		existing, err := OpenReadOnly(context.Background(), path)
		if err != nil {
			return nil, err
		}
		if err = existing.Close(); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, errors.New("database must be a regular file")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()
	if err = os.Chmod(path, 0600); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{DB: db}
	if err = s.initialize(); err != nil {
		db.Close()
		return nil, err
	}
	if err = s.InitializeMedia(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) initialize() error {
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=10000"} {
		if _, err := s.DB.Exec(q); err != nil {
			return err
		}
	}
	var version int
	if err := s.DB.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == schemaVersion {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("incompatible mirror schema version %d", version)
	}
	return s.Transaction(context.Background(), func(tx *sql.Tx) error { return createSchema(tx) })
}
func String(raw json.RawMessage, key string) string {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return ""
	}
	b := obj[key]
	var v string
	if json.Unmarshal(b, &v) == nil {
		return v
	}
	if len(b) > 0 && string(b) != "null" {
		return string(b)
	}
	return ""
}
func Identity(table string, raw json.RawMessage) string {
	fields := map[string][]string{"threads": {"id", "thread_fbid", "thread_id", "thread_key"}, "messages": {"id", "message_id"}, "users": {"id", "pk", "fbid_v2", "interop_messaging_user_fbid"}, "deltas": {"uq_seq_id"}}
	for _, key := range fields[table] {
		if v := String(raw, key); v != "" && v != "0" {
			return key + ":" + v
		}
	}
	return ""
}
func Hash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func Put(ctx context.Context, tx *sql.Tx, r Record) error {
	if r.Table != "threads" && r.Table != "messages" && r.Table != "users" && r.Table != "deltas" {
		return errors.New("invalid source table")
	}
	node, err := parseSource(r.Raw)
	if err != nil || node.shape != '{' {
		return errors.New("source must be a JSON object")
	}
	if r.Account == "" {
		return errors.New("missing account identity")
	}
	if r.Identity == "" {
		r.Identity = Identity(r.Table, r.Raw)
	}
	if r.Identity == "" {
		return fmt.Errorf("unidentifiable %s source", r.Table)
	}
	if r.Observed == 0 {
		r.Observed = time.Now().UnixMilli()
	}
	version, err := putVersion(ctx, tx, r.Account, r.Table, node)
	if err != nil {
		return err
	}
	var record int64
	err = tx.QueryRowContext(ctx, `INSERT INTO source_records(account_id,kind,source_identity,version_id,observed_at_ms,last_observed_at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,kind,source_identity,version_id) DO UPDATE SET observed_at_ms=min(observed_at_ms,excluded.observed_at_ms),last_observed_at_ms=max(last_observed_at_ms,excluded.last_observed_at_ms) RETURNING record_id`, r.Account, r.Table, r.Identity, version, r.Observed, r.Observed).Scan(&record)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO source_acquisitions(record_id,origin,acquired_thread_id,source_hash,observed_at_ms,last_observed_at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT(record_id,origin,acquired_thread_id,source_hash) DO UPDATE SET observed_at_ms=min(observed_at_ms,excluded.observed_at_ms),last_observed_at_ms=max(last_observed_at_ms,excluded.last_observed_at_ms)`, record, r.Origin, r.Thread, Hash(r.Raw), r.Observed, r.Observed)
	return err
}

func SaveState(ctx context.Context, tx *sql.Tx, account, scope, id string, state any) error {
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sync_state VALUES(?,?,?,?) ON CONFLICT(account_id,scope,scope_id) DO UPDATE SET state=excluded.state`, account, scope, id, string(b))
	return err
}
func (s *Store) State(ctx context.Context, account, scope, id string, out any) error {
	var b string
	err := s.queryRowContext(ctx, `SELECT state FROM sync_state WHERE account_id=? AND scope=? AND scope_id=?`, account, scope, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(b), out)
}
func (s *Store) Transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) SetState(ctx context.Context, account, scope, id string, value any) error {
	return s.Transaction(ctx, func(tx *sql.Tx) error { return SaveState(ctx, tx, account, scope, id, value) })
}
func (s *Store) Status(ctx context.Context, account string) ([]map[string]any, error) {
	rows, err := s.queryContext(ctx, `SELECT account_id,scope,scope_id,state FROM sync_state WHERE (?='' OR account_id=?) ORDER BY account_id,scope,scope_id`, account, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var a, scope, id string
		var b []byte
		if err = rows.Scan(&a, &scope, &id, &b); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"account_id": a, "scope": scope, "scope_id": id, "state": json.RawMessage(b)})
	}
	return out, rows.Err()
}
