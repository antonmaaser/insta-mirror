package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	DB     *sql.DB
	readTx *sql.Tx
}

type State struct {
	Revision        int64    `json:"revision,omitempty"`
	Anchor          string   `json:"anchor,omitempty"`
	Target          string   `json:"target,omitempty"`
	Candidate       string   `json:"candidate,omitempty"`
	Generation      int64    `json:"generation,omitempty"`
	PageHash        string   `json:"page_hash,omitempty"`
	Cursor          string   `json:"cursor,omitempty"`
	Complete        bool     `json:"complete"`
	SeqID           int64    `json:"seq_id,omitempty"`
	Timestamp       int64    `json:"timestamp_ms,omitempty"`
	LastSuccess     int64    `json:"last_success_ms,omitempty"`
	Error           string   `json:"error,omitempty"`
	Gaps            []string `json:"gaps,omitempty"`
	Connected       bool     `json:"connected,omitempty"`
	ObservedAt      int64    `json:"observed_at_ms,omitempty"`
	SubscribedSeqID int64    `json:"subscribed_seq_id,omitempty"`
	LatestSeqID     int64    `json:"latest_seq_id,omitempty"`
	Failures        int      `json:"failures,omitempty"`
	SessionSaved    bool     `json:"session_saved,omitempty"`
	LastValidated   int64    `json:"last_validated_ms,omitempty"`
	EstablishedAt   int64    `json:"established_at_ms,omitempty"`
	Coverage        string   `json:"coverage,omitempty"`
}

const schemaVersion = 6

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
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := url.Values{"_pragma": {"foreign_keys(1)", "busy_timeout(10000)"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{DB: db}
	if err = s.initialize(); err != nil {
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
	return s.Transaction(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(currentSchema); err != nil {
			return err
		}
		_, err := tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", schemaVersion))
		return err
	})
}
func Hash(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func EnsureAccount(ctx context.Context, tx *sql.Tx, account string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO accounts(account_id,created_at_ms,updated_at_ms) VALUES(?,?,?) ON CONFLICT DO NOTHING`, account, time.Now().UnixMilli(), time.Now().UnixMilli())
	return err
}
func cursorScope(scope string) string {
	switch scope {
	case "folder", "inbox":
		return "inbox"
	case "thread", "backfill":
		return "backfill"
	case "head":
		return "head"
	}
	return ""
}

const stateColumns = "revision,coalesce(anchor,''),coalesce(target,''),coalesce(candidate,''),generation,coalesce(page_hash,''),coalesce(cursor,''),complete,seq_id,timestamp_ms,last_success_ms,error,coalesce(connected,0),coalesce(observed_at_ms,0),coalesce(subscribed_seq_id,0),coalesce(latest_seq_id,0),coalesce(failures,0),coalesce(session_saved,0),coalesce(last_validated_ms,0),coalesce(established_at_ms,0),coalesce(coverage,'')"

func scanState(row interface{ Scan(...any) error }, out *State) error {
	return row.Scan(&out.Revision, &out.Anchor, &out.Target, &out.Candidate, &out.Generation, &out.PageHash, &out.Cursor, &out.Complete, &out.SeqID, &out.Timestamp, &out.LastSuccess, &out.Error, &out.Connected, &out.ObservedAt, &out.SubscribedSeqID, &out.LatestSeqID, &out.Failures, &out.SessionSaved, &out.LastValidated, &out.EstablishedAt, &out.Coverage)
}
func SaveState(ctx context.Context, tx *sql.Tx, account, scope, id string, value any) error {
	var v State
	if typed, ok := value.(State); ok {
		v = typed
	} else {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			return err
		}
	}
	if err := EnsureAccount(ctx, tx, account); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO operational_state(account_id,scope,scope_id,revision,anchor,target,candidate,generation,page_hash,cursor,complete,seq_id,timestamp_ms,last_success_ms,error,connected,observed_at_ms,subscribed_seq_id,latest_seq_id,failures,session_saved,last_validated_ms,established_at_ms,coverage) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,scope,scope_id) DO UPDATE SET revision=excluded.revision,anchor=excluded.anchor,target=excluded.target,candidate=excluded.candidate,generation=excluded.generation,page_hash=excluded.page_hash,cursor=excluded.cursor,complete=excluded.complete,seq_id=excluded.seq_id,timestamp_ms=excluded.timestamp_ms,last_success_ms=excluded.last_success_ms,error=excluded.error,connected=excluded.connected,observed_at_ms=excluded.observed_at_ms,subscribed_seq_id=excluded.subscribed_seq_id,latest_seq_id=excluded.latest_seq_id,failures=excluded.failures,session_saved=excluded.session_saved,last_validated_ms=excluded.last_validated_ms,established_at_ms=excluded.established_at_ms,coverage=excluded.coverage`, account, scope, id, v.Revision, v.Anchor, v.Target, v.Candidate, v.Generation, v.PageHash, v.Cursor, v.Complete, v.SeqID, v.Timestamp, v.LastSuccess, v.Error, v.Connected, v.ObservedAt, v.SubscribedSeqID, v.LatestSeqID, v.Failures, v.SessionSaved, v.LastValidated, v.EstablishedAt, v.Coverage)
	if err != nil {
		return err
	}
	if cs := cursorScope(scope); cs != "" {
		hash, err := hex.DecodeString(v.PageHash)
		if err != nil {
			return err
		}
		anchor := v.Anchor
		if anchor != "" && cs != "inbox" {
			local, err := strconv.ParseInt(anchor, 10, 64)
			if err != nil {
				return err
			}
			if err = tx.QueryRowContext(ctx, `SELECT remote_id FROM message_identities WHERE account_id=? AND message_id=? AND namespace='mid' ORDER BY remote_id LIMIT 1`, account, local).Scan(&anchor); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO sync_cursors(account_id,scope,scope_id,revision,cursor,anchor_remote_id,page_fingerprint,complete,last_success_ms,target,candidate,generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,scope,scope_id) DO UPDATE SET revision=excluded.revision,cursor=excluded.cursor,anchor_remote_id=excluded.anchor_remote_id,page_fingerprint=excluded.page_fingerprint,complete=excluded.complete,last_success_ms=excluded.last_success_ms,target=excluded.target,candidate=excluded.candidate,generation=excluded.generation`, account, cs, id, v.Revision, v.Cursor, anchor, hash, v.Complete, v.LastSuccess, v.Target, v.Candidate, v.Generation)
		if err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM operational_gaps WHERE account_id=? AND scope=? AND scope_id=?`, account, scope, id); err != nil {
		return err
	}
	for i, gap := range v.Gaps {
		if _, err = tx.ExecContext(ctx, `INSERT INTO operational_gaps VALUES(?,?,?,?,?)`, account, scope, id, i, gap); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) State(ctx context.Context, account, scope, id string, out *State) error {
	return readState(ctx, s, account, scope, id, out)
}

type stateReader interface {
	queryRowContext(context.Context, string, ...any) *sql.Row
	queryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readState(ctx context.Context, q stateReader, account, scope, id string, out *State) error {
	*out = State{}
	err := scanState(q.queryRowContext(ctx, "SELECT "+stateColumns+" FROM operational_state WHERE account_id=? AND scope=? AND scope_id=?", account, scope, id), out)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if cs := cursorScope(scope); cs != "" {
		var hash []byte
		err = q.queryRowContext(ctx, `SELECT revision,cursor,anchor_remote_id,page_fingerprint,complete,last_success_ms,target,candidate,generation FROM sync_cursors WHERE account_id=? AND scope=? AND scope_id=?`, account, cs, id).Scan(&out.Revision, &out.Cursor, &out.Anchor, &hash, &out.Complete, &out.LastSuccess, &out.Target, &out.Candidate, &out.Generation)
		if err != nil {
			return err
		}
		if out.Anchor != "" && cs != "inbox" {
			var local int64
			if err = q.queryRowContext(ctx, `SELECT message_id FROM message_identities WHERE account_id=? AND namespace='mid' AND remote_id=?`, account, out.Anchor).Scan(&local); err != nil {
				return err
			}
			out.Anchor = strconv.FormatInt(local, 10)
		}
		out.PageHash = hex.EncodeToString(hash)
	}
	rows, err := q.queryContext(ctx, `SELECT detail FROM operational_gaps WHERE account_id=? AND scope=? AND scope_id=? ORDER BY ordinal`, account, scope, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var gap string
		if err = rows.Scan(&gap); err != nil {
			return err
		}
		out.Gaps = append(out.Gaps, gap)
	}
	return rows.Err()
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
	rows, err := s.queryContext(ctx, `SELECT account_id,scope,scope_id FROM operational_state WHERE (?='' OR account_id=?) ORDER BY account_id,scope,scope_id`, account, account)
	if err != nil {
		return nil, err
	}
	type key struct{ a, scope, id string }
	var keys []key
	for rows.Next() {
		var k key
		if err = rows.Scan(&k.a, &k.scope, &k.id); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, k)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, k := range keys {
		var v State
		if err = s.State(ctx, k.a, k.scope, k.id, &v); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"account_id": k.a, "scope": k.scope, "scope_id": k.id, "state": v})
	}
	return out, nil
}
func ReadState(ctx context.Context, tx *sql.Tx, account, scope, id string, out *State) error {
	return readState(ctx, &Store{readTx: tx}, account, scope, id, out)
}
