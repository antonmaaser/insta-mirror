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
	Table                 string   `json:"kind"`
	Entity                int64    `json:"entity_id,omitempty"`
	EntityReference       string   `json:"entity_reference,omitempty"`
	Sequence              int64    `json:"source_sequence,omitempty"`
	Matches               []string `json:"matching_source_references,omitempty"`
	HashScope             string   `json:"source_hash_scope"`
	Observation           int64    `json:"observation_id,omitempty"`
	AcquisitionsTruncated bool     `json:"acquisitions_truncated,omitempty"`
	Context               string   `json:"-"`
	watermark             int64
	RowID                 int64           `json:"record_id"`
	Account               string          `json:"account_id"`
	Identity              string          `json:"source_identity"`
	Thread                string          `json:"acquired_thread_id,omitempty"`
	Origin                string          `json:"origin"`
	Observed              int64           `json:"observed_at_ms"`
	LastObserved          int64           `json:"last_observed_at_ms"`
	Hash                  string          `json:"source_hash"`
	Raw                   json.RawMessage `json:"raw,omitempty"`
	Version               int64           `json:"-"`
	ContentHash           string          `json:"normalized_content_hash"`
	Representation        string          `json:"representation"`
	Acquisitions          []Acquisition   `json:"acquisitions,omitempty"`
}

// Acquisition identifies how a version was observed, independently of its content.
type Acquisition struct {
	RootKind       string   `json:"root_kind"`
	ExcludedFields []string `json:"excluded_fields,omitempty"`
	RootVersion    int64    `json:"root_version_id"`
	Event          string   `json:"event_identity,omitempty"`
	ID             int64    `json:"observation_id"`
	Origin         string   `json:"origin"`
	Thread         string   `json:"acquired_thread_id,omitempty"`
	Hash           string   `json:"source_hash"`
	Observed       int64    `json:"observed_at_ms"`
	LastObserved   int64    `json:"last_observed_at_ms"`
}

type State struct {
	Revision    int64    `json:"revision,omitempty"`
	Anchor      string   `json:"anchor,omitempty"`
	Target      string   `json:"target,omitempty"`
	Candidate   string   `json:"candidate,omitempty"`
	Generation  int64    `json:"generation,omitempty"`
	PageHash    string   `json:"page_hash,omitempty"`
	Cursor      string   `json:"cursor,omitempty"`
	Complete    bool     `json:"complete"`
	SeqID       int64    `json:"seq_id,omitempty"`
	Timestamp   int64    `json:"timestamp_ms,omitempty"`
	LastSuccess int64    `json:"last_success_ms,omitempty"`
	Error       string   `json:"error,omitempty"`
	Gaps        []string `json:"gaps,omitempty"`
}

const schemaVersion = 4

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
func Identity(kind string, raw json.RawMessage) string {
	node, err := parseSource(raw)
	if err != nil {
		return ""
	}
	if kind == "deltas" {
		if value := nodeValue(node, "uq_seq_id"); value != "" {
			return "uq_seq_id:" + value
		}
		return ""
	}
	for _, field := range sourceColumns[kind] {
		if value := nodeValue(node, field); value != "" {
			return field + ":" + value
		}
	}
	return ""
}
func Hash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }

// Put archives one acquisition root. Embedded content inherits provenance via
// source_relations, rather than allocating an observation for every descendant.
func Put(ctx context.Context, tx *sql.Tx, r Record) error {
	if r.Table != "threads" && r.Table != "messages" && r.Table != "users" && r.Table != "deltas" && r.Table != "mailbox_pages" {
		return errors.New("invalid source table")
	}
	node, err := parseSource(r.Raw)
	if err != nil || node.shape != '{' {
		return errors.New("source must be a JSON object")
	}
	if r.Table == "mailbox_pages" {
		if r.Hash == "" {
			r.Hash = Hash(r.Raw)
		}
		stripMailboxToken(node)
		r.Raw = node.bytes()
	}
	if r.Account == "" {
		return errors.New("missing account identity")
	}
	if r.Observed == 0 {
		r.Observed = time.Now().UnixMilli()
	}
	version, err := putVersion(ctx, tx, r.Account, r.Table, node)
	if err != nil {
		return err
	}
	contextID := r.Context
	if col := ownerColumn(r.Table); col != "" {
		var owner sql.NullInt64
		if err = tx.QueryRowContext(ctx, `SELECT `+col+` FROM source_versions WHERE version_id=?`, version).Scan(&owner); err != nil {
			return err
		}
		if owner.Valid {
			var canonical int64
			if err = tx.QueryRowContext(ctx, `SELECT coalesce(merged_into,entity_id) FROM `+r.Table+` WHERE account_id=? AND entity_id=?`, r.Account, owner.Int64).Scan(&canonical); err != nil {
				return err
			}
			contextID += fmt.Sprintf(":%s:%d", r.Table, canonical)
		} else {
			contextID += fmt.Sprintf(":version:%d", version)
		}
	} else if contextID == "" {
		contextID = r.Identity
		if contextID == "" {
			contextID = Identity(r.Table, r.Raw)
		}
	}

	event := ""
	if r.Table == "deltas" {
		event = r.Identity
		if event == "" {
			event = Identity("deltas", r.Raw)
		}
	}
	hash := r.Hash
	if hash == "" {
		hash = Hash(r.Raw)
	}
	var previous, oldVersion int64
	var oldHash string
	if event != "" {
		err = tx.QueryRowContext(ctx, `SELECT observation_id,version_id,source_hash FROM source_observations WHERE account_id=? AND event_identity=?`, r.Account, event).Scan(&previous, &oldVersion, &oldHash)
		if err == nil && oldVersion != version {
			// Preserve conflicting event payloads while leaving the original sequence
			// identity and checkpoint intact. Repeated conflicts reuse their evidence.
			conflict := event + ":conflict:" + hash
			if _, err = tx.ExecContext(ctx, `INSERT INTO source_observations(account_id,version_id,origin,context,acquired_thread_id,source_hash,observed_at_ms,last_observed_at_ms,event_identity) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,event_identity) WHERE event_identity!='' DO UPDATE SET last_observed_at_ms=max(last_observed_at_ms,excluded.last_observed_at_ms)`, r.Account, version, r.Origin, contextID, r.Thread, hash, r.Observed, r.Observed, conflict); err != nil {
				return err
			}
			return SaveState(ctx, tx, r.Account, "realtime", "conflict", State{Error: "conflicting payloads for a source event sequence; checkpoint blocked"})
		}
	} else {
		err = tx.QueryRowContext(ctx, `SELECT observation_id,version_id,source_hash FROM source_observations WHERE account_id=? AND origin=? AND context=? ORDER BY observation_id DESC LIMIT 1`, r.Account, r.Origin, contextID).Scan(&previous, &oldVersion, &oldHash)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && oldVersion == version && (oldHash == hash || event != "") {
		_, err = tx.ExecContext(ctx, `UPDATE source_observations SET last_observed_at_ms=max(last_observed_at_ms,?) WHERE observation_id=?`, r.Observed, previous)
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO source_observations(account_id,version_id,origin,context,acquired_thread_id,source_hash,observed_at_ms,last_observed_at_ms,event_identity) VALUES(?,?,?,?,?,?,?,?,?)`, r.Account, version, r.Origin, contextID, r.Thread, hash, r.Observed, r.Observed, event)
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
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	failures, err := s.queryContext(ctx, `SELECT account_id,kind,version_id,source_error FROM source_versions WHERE source_error!='' AND (?='' OR account_id=?) ORDER BY version_id DESC LIMIT 20`, account, account)
	if err != nil {
		return nil, err
	}
	defer failures.Close()
	for failures.Next() {
		var r Record
		var label string
		if err = failures.Scan(&r.Account, &r.Table, &r.RowID, &label); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"account_id": r.Account, "scope": "retained_source_boundary", "current_state": false, "error": label, "source_reference": SourceRef(r)})
	}
	return out, failures.Err()
}

// ReadState reads on the same transaction that stores observations/checkpoints.
func ReadState(ctx context.Context, tx *sql.Tx, account, scope, id string, out any) error {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT state FROM sync_state WHERE account_id=? AND scope=? AND scope_id=?`, account, scope, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(raw), out)
}
