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

	_ "modernc.org/sqlite"
)

type Store struct {
	DB     *sql.DB
	readTx *sql.Tx
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

const schemaVersion = 5

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

func SaveState(ctx context.Context, tx *sql.Tx, account, scope, id string, state any) error {
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sync_state VALUES(?,?,?,?) ON CONFLICT(account_id,scope,scope_id) DO UPDATE SET state=excluded.state`, account, scope, id, string(b))
	return err
}
func (s *Store) State(ctx context.Context, account, scope, id string, out *State) error {
	var b string
	err := s.queryRowContext(ctx, `SELECT state FROM sync_state WHERE account_id=? AND scope=? AND scope_id=?`, account, scope, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		*out = State{}
		return nil
	}
	if err != nil {
		return err
	}
	var decoded State
	if err := json.Unmarshal([]byte(b), &decoded); err != nil {
		return err
	}
	*out = decoded
	return nil
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

// ReadState reads on the same transaction that stores observations/checkpoints.
func ReadState(ctx context.Context, tx *sql.Tx, account, scope, id string, out *State) error {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT state FROM sync_state WHERE account_id=? AND scope=? AND scope_id=?`, account, scope, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		*out = State{}
		return nil
	}
	if err != nil {
		return err
	}
	var decoded State
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return err
	}
	*out = decoded
	return nil
}
